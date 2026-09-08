// Linux prototype: a supervisor of processes it creates, not a Vela authority.
#define _GNU_SOURCE
#include <elf.h>
#include <errno.h>
#include <grp.h>
#include <linux/audit.h>
#include <linux/ptrace.h>
#include <sched.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/prctl.h>
#include <sys/syscall.h>
#include <sys/uio.h>
#include <sys/wait.h>
#include <unistd.h>
#if defined(__aarch64__)
#include <asm/ptrace.h>
#define EXPECTED_ARCH AUDIT_ARCH_AARCH64
#elif defined(__x86_64__)
#include <sys/user.h>
#define EXPECTED_ARCH AUDIT_ARCH_X86_64
#else
#error This prototype supports only Linux arm64 and amd64
#endif

// Use the variadic libc declaration without mixing the glibc/kernel enums.
extern long ptrace(int request, ...);
extern char **environ;

struct task { pid_t pid; int skipped_errno; };
static struct task tasks[128];
static pid_t original;
static int initial_exec;

static unsigned credential(const char *text) {
    char *end;
    errno = 0;
    unsigned long value = strtoul(text, &end, 10);
    if (errno || *end || value == 0 || value >= UINT32_MAX) exit(2);
    return (unsigned)value;
}

static void fail(const char *message) {
    perror(message);
    // EXITKILL is the kernel backstop. Explicitly signal known tasks too.
    for (size_t i = 0; i < 128; i++) if (tasks[i].pid) kill(tasks[i].pid, SIGKILL);
    exit(2);
}

static struct task *lookup(pid_t pid) {
    for (size_t i = 0; i < 128; i++) if (tasks[i].pid == pid) return &tasks[i];
    for (size_t i = 0; i < 128; i++) if (!tasks[i].pid) {
        tasks[i] = (struct task){.pid = pid};
        return &tasks[i];
    }
    errno = EOVERFLOW;
    fail("task limit");
    return NULL;
}

static pid_t tgid(pid_t pid) {
    char path[64], line[256];
    snprintf(path, sizeof(path), "/proc/%ld/status", (long)pid);
    FILE *file = fopen(path, "r");
    if (!file) fail("open traced task status");
    long result = 0;
    while (fgets(line, sizeof(line), file)) if (sscanf(line, "Tgid: %ld", &result) == 1) break;
    if (fclose(file) || result <= 0) fail("read traced task tgid");
    return (pid_t)result;
}

#ifndef DISABLE_CLONE_GUARD
static void skip_syscall(pid_t pid) {
#if defined(__aarch64__)
    int number = -1;
    struct iovec vector = {.iov_base = &number, .iov_len = sizeof(number)};
    if (ptrace(PTRACE_SETREGSET, pid, (void *)NT_ARM_SYSTEM_CALL, &vector)) fail("skip arm64 syscall");
#else
    struct user_regs_struct regs;
    if (ptrace(PTRACE_GETREGS, pid, NULL, &regs)) fail("get amd64 registers");
    regs.orig_rax = (unsigned long long)-1;
    if (ptrace(PTRACE_SETREGS, pid, NULL, &regs)) fail("skip amd64 syscall");
#endif
}
#endif

static void return_errno(pid_t pid, int value) {
#if defined(__aarch64__)
    struct user_pt_regs regs;
    struct iovec vector = {.iov_base = &regs, .iov_len = sizeof(regs)};
    if (ptrace(PTRACE_GETREGSET, pid, (void *)NT_PRSTATUS, &vector)) fail("get arm64 registers");
    regs.regs[0] = (uint64_t)-(int64_t)value;
    if (ptrace(PTRACE_SETREGSET, pid, (void *)NT_PRSTATUS, &vector)) fail("return arm64 errno");
#else
    struct user_regs_struct regs;
    if (ptrace(PTRACE_GETREGS, pid, NULL, &regs)) fail("get amd64 return registers");
    regs.rax = (unsigned long long)-(long long)value;
    if (ptrace(PTRACE_SETREGS, pid, NULL, &regs)) fail("return amd64 errno");
#endif
}

static void syscall_stop(struct task *task) {
    struct ptrace_syscall_info info;
    memset(&info, 0, sizeof(info));
    long size = ptrace(PTRACE_GET_SYSCALL_INFO, task->pid, sizeof(info), &info);
    if (size < 0 || info.arch != EXPECTED_ARCH) fail("syscall ABI");
    if (info.op == PTRACE_SYSCALL_INFO_EXIT) {
        if (task->skipped_errno) {
            return_errno(task->pid, task->skipped_errno);
            task->skipped_errno = 0;
        }
        return;
    }
    if (info.op != PTRACE_SYSCALL_INFO_ENTRY || task->skipped_errno) fail("syscall entry sequence");
    uint64_t number = info.entry.nr;
#if defined(__x86_64__)
    if (number & 0x40000000U) fail("x32 ABI is not supported");
#endif
    if (number == SYS_execve || number == SYS_execveat) {
        if (tgid(task->pid) == original) {
            if (!initial_exec) {
                initial_exec = 1;
            } else {
                fprintf(stderr, "REJECT root-exec %ld\n", (long)task->pid);
                for (size_t i = 0; i < 128; i++) if (tasks[i].pid) kill(tasks[i].pid, SIGKILL);
                exit(3);
            }
        }
    }
#ifndef DISABLE_CLONE_GUARD
    // clone's flags are scalar syscall registers. clone3's pointed-to arguments
    // can be changed by another thread between ptrace stop and kernel copy.
    // Do not inspect those bytes as authority: force the standard ENOSYS fallback.
    if (number == SYS_clone3) task->skipped_errno = ENOSYS;
    if (number == SYS_clone && (info.entry.args[0] & CLONE_UNTRACED)) task->skipped_errno = EPERM;
    if (task->skipped_errno) {
        fprintf(stderr, "DENY clone %ld %d\n", (long)task->pid, task->skipped_errno);
        skip_syscall(task->pid);
    }
#endif
}

int main(int argc, char **argv) {
    if (getuid() != 0) return 2;
    int offset = 1;
    if (argc > 1 && strcmp(argv[1], "--new-pid") == 0) {
        if (unshare(CLONE_NEWPID)) fail("create target PID namespace");
        offset++;
    }
    if (argc < offset + 3) return 2;
    unsigned uid = credential(argv[offset]), gid = credential(argv[offset + 1]);
    char **arguments = argv + offset + 2;
    setbuf(stderr, NULL);
    original = fork();
    if (original < 0) fail("fork target");
    if (original == 0) {
        if (ptrace(PTRACE_TRACEME, 0, NULL, NULL)) fail("PTRACE_TRACEME");
        raise(SIGSTOP);
        if (setgroups(0, NULL) || setgid(gid) || setuid(uid)) fail("drop credentials");
        if (prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0)) fail("no_new_privs");
        execve(arguments[0], arguments, environ);
        fail("initial exec");
    }
    lookup(original);
    int status;
    if (waitpid(original, &status, 0) != original || !WIFSTOPPED(status)) fail("initial stop");
    long options = PTRACE_O_TRACESYSGOOD | PTRACE_O_TRACEEXEC | PTRACE_O_TRACECLONE |
                   PTRACE_O_TRACEFORK | PTRACE_O_TRACEVFORK;
#ifndef DISABLE_EXITKILL
    options |= PTRACE_O_EXITKILL;
#endif
    if (ptrace(PTRACE_SETOPTIONS, original, NULL, options)) fail("set trace options");
    fprintf(stderr, "ROOT %ld\n", (long)original);
    if (ptrace(PTRACE_SYSCALL, original, NULL, NULL)) fail("start target");
    for (;;) {
        pid_t pid = waitpid(-1, &status, __WALL);
        if (pid < 0 && errno == EINTR) continue;
        if (pid < 0) fail("wait traced tasks");
        struct task *task = lookup(pid);
        if (WIFEXITED(status) || WIFSIGNALED(status)) {
            task->pid = 0;
            if (pid == original) return WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status);
            continue;
        }
        if (!WIFSTOPPED(status)) fail("unexpected task event");
        unsigned event = (unsigned)status >> 16;
        int deliver = 0;
        if (event) {
            if (event == PTRACE_EVENT_CLONE || event == PTRACE_EVENT_FORK || event == PTRACE_EVENT_VFORK) {
                unsigned long child;
                if (ptrace(PTRACE_GETEVENTMSG, pid, NULL, &child)) fail("new task event");
                lookup((pid_t)child);
                fprintf(stderr, "CHILD %lu\n", child);
            } else if (event == PTRACE_EVENT_EXEC) {
                // Exec entry is checked before the kernel changes any image.
                fprintf(stderr, "EXEC %ld\n", (long)pid);
            } else {
                fail("unexpected ptrace event");
            }
        } else if (WSTOPSIG(status) == (SIGTRAP | 0x80)) {
            syscall_stop(task);
        } else if (WSTOPSIG(status) != SIGSTOP) {
            deliver = WSTOPSIG(status);
        }
        if (ptrace(PTRACE_SYSCALL, pid, NULL, deliver)) fail("continue syscall");
    }
}
