#define _GNU_SOURCE
#include <errno.h>
#include <linux/sched.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/prctl.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <unistd.h>

extern char **environ;
static char **original_argv;

static void fail(const char *message) { perror(message); exit(1); }

static void *thread_exec(void *unused) {
    (void)unused;
    execve("/exec-probe", original_argv, environ);
    fail("thread exec");
    return NULL;
}

static int clone_exec(void *unused) { thread_exec(unused); return 1; }

int main(int argc, char **argv) {
    if (getuid() != 65534 || getgid() != 65534) return 2;
    if (prctl(PR_SET_DUMPABLE, 0) || prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0)) fail("protect");
    setbuf(stdout, NULL);
    if (argc == 2 && strcmp(argv[1], "once") == 0) { puts("ONCE"); return 0; }
    if (argc == 2 && strcmp(argv[1], "child") == 0) {
        printf("CHILD_READY %ld\n", (long)getpid());
        for (;;) syscall(SYS_ppoll, NULL, 0, NULL, NULL, 0);
    }
    original_argv = argv;
    printf("READY %ld\n", (long)getpid());
    char command[32];
    while (fgets(command, sizeof(command), stdin)) {
        if (strcmp(command, "exec\n") == 0) thread_exec(NULL);
        else if (strcmp(command, "execat\n") == 0) {
            syscall(SYS_execveat, -100, "/exec-probe", original_argv, environ, 0);
            fail("execveat");
        } else if (strcmp(command, "thread-exec\n") == 0) {
            pthread_t thread;
            if (pthread_create(&thread, NULL, thread_exec, NULL)) fail("pthread_create");
            if (pthread_join(thread, NULL)) fail("pthread_join");
        } else if (strcmp(command, "untraced-exec\n") == 0) {
            size_t size = 1 << 20;
            void *stack = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_STACK, -1, 0);
            if (stack == MAP_FAILED) fail("clone stack");
            int flags = CLONE_VM | CLONE_THREAD | CLONE_SIGHAND | CLONE_UNTRACED;
            int result = clone(clone_exec, (char *)stack + size, flags, NULL);
            if (result < 0) {
                if (errno != EPERM) fail("unexpected clone rejection");
                if (munmap(stack, size)) fail("release clone stack");
                puts("UNTRACED_BLOCKED");
            } else {
                // An escaped thread replaces this entire process, including its
                // leader. Do not race the replacement by reading stdin here.
                for (;;) syscall(SYS_ppoll, NULL, 0, NULL, NULL, 0);
            }
        } else if (strcmp(command, "clone3\n") == 0) {
            struct clone_args args = {.exit_signal = SIGCHLD};
            long result = syscall(SYS_clone3, &args, sizeof(args));
            if (result >= 0 || errno != ENOSYS) fail("clone3 not forced to ENOSYS");
            puts("CLONE3_FALLBACK");
        } else if (strcmp(command, "fork\n") == 0) {
            pid_t child = fork();
            if (child < 0) fail("fork backend");
            if (!child) {
                char *arguments[] = {"/exec-probe", "child", NULL};
                execve(arguments[0], arguments, environ);
                _exit(1);
            }
            printf("FORKED %ld\n", (long)child);
        } else if (strcmp(command, "exit\n") == 0) return 0;
        else return 3;
    }
    return 4;
}
