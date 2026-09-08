// Linux-only negative experiment. This is not a Vela startup authenticator.
#define _GNU_SOURCE
#include <errno.h>
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

#ifndef IMAGE_LABEL
#error IMAGE_LABEL must identify the independently built executable
#endif

extern char **environ;

static void fail(const char *operation) {
    perror(operation);
    exit(1);
}

static int hold_mm(void *unused) {
    (void)unused;
    // Avoid libc/TLS writes in this separate process sharing the parent's mm.
    for (;;) syscall(SYS_ppoll, NULL, 0, NULL, NULL, 0);
    return 0;
}

int main(int argc, char **argv) {
    if (argc != 2 || strcmp(argv[1], "target") != 0 || getuid() == 0)
        return 2;
    if (prctl(PR_SET_DUMPABLE, 0) != 0) fail("PR_SET_DUMPABLE");
    if (prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0) fail("PR_SET_NO_NEW_PRIVS");
    setbuf(stdout, NULL);
    printf("READY %s %ld\n", IMAGE_LABEL, (long)getpid());
    char command[32];
    while (fgets(command, sizeof(command), stdin) != NULL) {
        if (strcmp(command, "share\n") == 0 || strcmp(command, "fork\n") == 0) {
            pid_t holder;
            if (command[0] == 's') {
                const size_t stack_size = 1 << 20;
                void *stack = mmap(NULL, stack_size, PROT_READ | PROT_WRITE,
                                   MAP_PRIVATE | MAP_ANONYMOUS | MAP_STACK, -1, 0);
                if (stack == MAP_FAILED) fail("mmap");
                holder = clone(hold_mm, (char *)stack + stack_size,
                               CLONE_VM | SIGCHLD, NULL);
            } else {
                holder = fork();
                if (holder == 0) _exit(hold_mm(NULL));
            }
            if (holder < 0) fail("create holder");
            printf("HOLDER %ld\n", (long)holder);
        } else if (strcmp(command, "exec-a\n") == 0 || strcmp(command, "exec-b\n") == 0) {
            // Keep argv and environ byte-for-byte unchanged, including argv[0].
            execve(command[5] == 'a' ? "/probe-a" : "/probe-b", argv, environ);
            fail("execve");
        } else if (strcmp(command, "reap\n") == 0) {
            int status;
            pid_t child;
            do { child = waitpid(-1, &status, 0); } while (child < 0 && errno == EINTR);
            if (child < 0 || !WIFSIGNALED(status) || WTERMSIG(status) != SIGKILL)
                fail("reap holder");
            printf("REAPED %ld\n", (long)child);
        } else if (strcmp(command, "exit\n") == 0) {
            return 0;
        } else {
            return 3;
        }
    }
    return 4;
}
