// Linux prototype: a supervisor of processes it creates, not a Vela authority.
#define _GNU_SOURCE
#include <elf.h>
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <linux/audit.h>
#include <linux/ptrace.h>
#include <sched.h>
#include <signal.h>
#include <poll.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/prctl.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <sys/time.h>
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
static int custody_fd = -1;
static int notifications[2] = {-1, -1};
static const char custody_protocol[] = "vela-exec-custody-v1";
enum { custody_size = sizeof(custody_protocol) + 32 };

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

static void notify_child(int number) {
    (void)number;
    int saved = errno;
    char value = 1;
    // A full pipe already wakes poll. No heap/stdio operations in the handler.
    (void)write(notifications[1], &value, 1);
    errno = saved;
}

static void start_notifications(void) {
    if (pipe2(notifications, O_CLOEXEC | O_NONBLOCK)) fail("notification pipe");
    struct sigaction action = {.sa_handler = notify_child};
    if (sigemptyset(&action.sa_mask) || sigaction(SIGCHLD, &action, NULL)) fail("SIGCHLD notification");
}

static int valid_control(const char *packet, ssize_t size, char operation) {
    return size == custody_size && !memcmp(packet, custody_protocol, sizeof(custody_protocol) - 1) &&
           packet[sizeof(custody_protocol) - 1] == operation;
}

static void send_control(char *packet, char operation) {
    packet[sizeof(custody_protocol) - 1] = operation;
    if (send(custody_fd, packet, custody_size, MSG_NOSIGNAL) != custody_size) fail("send custody control");
}

static void offer_custody(void) {
    struct timeval timeout = {.tv_sec = 5};
    int kind;
    socklen_t length = sizeof(kind);
    if (getsockopt(custody_fd, SOL_SOCKET, SO_TYPE, &kind, &length) || kind != SOCK_SEQPACKET ||
        setsockopt(custody_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) ||
        setsockopt(custody_fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout))) fail("custody socket");
    char packet[custody_size];
    ssize_t count = recv(custody_fd, packet, sizeof(packet), MSG_TRUNC);
    if (!valid_control(packet, count, 'C')) fail("custody challenge");
    int target = (int)syscall(SYS_pidfd_open, original, 0);
    if (target < 0) fail("open original target pidfd");
    union { struct cmsghdr alignment; char bytes[CMSG_SPACE(sizeof(int))]; } ancillary = {0};
    struct iovec vector = {.iov_base = packet, .iov_len = sizeof(packet)};
    struct msghdr message = {.msg_iov = &vector, .msg_iovlen = 1, .msg_control = ancillary.bytes, .msg_controllen = sizeof(ancillary.bytes)};
    struct cmsghdr *header = CMSG_FIRSTHDR(&message);
    header->cmsg_level = SOL_SOCKET;
    header->cmsg_type = SCM_RIGHTS;
    header->cmsg_len = CMSG_LEN(sizeof(int));
    memcpy(CMSG_DATA(header), &target, sizeof(target));
    packet[sizeof(custody_protocol) - 1] = 'O';
    if (sendmsg(custody_fd, &message, MSG_NOSIGNAL) != (ssize_t)sizeof(packet)) fail("offer original pidfd");
    if (close(target)) fail("close offered descriptor copy");
    char acknowledgement[custody_size];
    count = recv(custody_fd, acknowledgement, sizeof(acknowledgement), MSG_TRUNC);
    if (!valid_control(acknowledgement, count, 'A') ||
        memcmp(acknowledgement + sizeof(custody_protocol), packet + sizeof(custody_protocol), 32)) fail("custody acknowledgement");
    send_control(packet, 'R');
}

static void service_control(void) {
    char packet[custody_size];
    ssize_t count = recv(custody_fd, packet, sizeof(packet), MSG_DONTWAIT | MSG_TRUNC);
    if (count < 0 && (errno == EAGAIN || errno == EINTR)) return;
    if (!valid_control(packet, count, 'P')) fail("custody channel lost or malformed");
    send_control(packet, 'L');
}

static void wait_activity(void) {
    struct pollfd watched[] = {{.fd = custody_fd, .events = POLLIN}, {.fd = notifications[0], .events = POLLIN}};
    if (poll(watched, 2, -1) < 0 && errno != EINTR) fail("wait custody/trace activity");
    char data[256];
    while (read(notifications[0], data, sizeof(data)) > 0) {}
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
    while (offset < argc) {
        if (strcmp(argv[offset], "--new-pid") == 0) {
            if (unshare(CLONE_NEWPID)) fail("create target PID namespace");
        } else if (strcmp(argv[offset], "--custody-fd3") == 0) {
            custody_fd = 3;
            if (fcntl(custody_fd, F_SETFD, FD_CLOEXEC)) fail("protect custody descriptor");
        } else break;
        offset++;
    }
    if (argc < offset + 3) return 2;
    unsigned uid = credential(argv[offset]), gid = credential(argv[offset + 1]);
    char **arguments = argv + offset + 2;
    setbuf(stderr, NULL);
    original = fork();
    if (original < 0) fail("fork target");
    if (original == 0) {
        // No target code may inherit the Node/creator control endpoint, even
        // before the first executable. The ptrace parent alone owns it.
        if (custody_fd >= 0 && close(custody_fd)) _exit(2);
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
    if (custody_fd >= 0) {
        start_notifications();
        offer_custody();
    }
    if (ptrace(PTRACE_SYSCALL, original, NULL, NULL)) fail("start target");
    for (;;) {
        if (custody_fd >= 0) service_control();
        pid_t pid = waitpid(-1, &status, __WALL | (custody_fd >= 0 ? WNOHANG : 0));
        if (pid == 0) {
            wait_activity();
            continue;
        }
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
