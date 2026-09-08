"""Exercise the supervisor prototype in a disposable, privileged Linux image."""
import json
import os
import select
import signal
import subprocess
import time


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def read_line(stream):
    require(select.select([stream], [], [], 10)[0], "event timed out")
    result = stream.readline().decode().strip()
    require(result, "unexpected event EOF")
    return result


def send(process, command):
    process.stdin.write((command + "\n").encode())
    process.stdin.flush()


def case(name, command=None, observer="/exec-observer", crash=False):
    log_path = "/tmp/observer-" + name + ".log"
    with open(log_path, "wb") as log:
        process = subprocess.Popen([observer, "65534", "65534", "/exec-probe"], stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE, stderr=log, bufsize=0)
        root_fd = child_fd = None
        try:
            event = read_line(process.stdout).split()
            require(event[0] == "READY" and len(event) == 2, "target not ready")
            root = int(event[1])
            root_fd = os.pidfd_open(root)
            if crash or name == "normal-exit-with-child":
                send(process, "fork")
                events = [read_line(process.stdout).split(), read_line(process.stdout).split()]
                require({row[0] for row in events} == {"FORKED", "CHILD_READY"}, "backend did not exec")
                child = int(events[0][1])
                require(child == int(events[1][1]), "wrong child")
                child_fd = os.pidfd_open(child)
            if crash:
                process.kill()
            elif command:
                send(process, command)
                if name == "untraced-disabled":
                    require(read_line(process.stdout) == f"READY {root}", "disabled guard did not expose escape")
                    # The untraced replacement may survive its original observer.
                    signal.pidfd_send_signal(root_fd, signal.SIGKILL)
                elif command in ("untraced-exec", "clone3"):
                    expected = "UNTRACED_BLOCKED" if command == "untraced-exec" else "CLONE3_FALLBACK"
                    require(read_line(process.stdout) == expected, "unsafe clone was not blocked")
                    require(not select.select([root_fd], [], [], 0)[0], "clone denial killed healthy target")
                    send(process, "exit")
            else:
                send(process, "exit")
            code = process.wait(timeout=10)
            if crash:
                require(code == -signal.SIGKILL, "observer did not crash")
            elif command in ("exec", "execat", "thread-exec"):
                require(code == 3, "original exec was not rejected before execution")
                require(process.stdout.read() == b"", "replacement image ran before rejection")
            elif name != "untraced-disabled":
                require(code == 0, "supervisor/target normal path failed")
            if name == "observer-crash-without-exitkill":
                require(not select.select([root_fd, child_fd], [], [], 0)[0], "disabled EXITKILL did not expose surviving tasks")
            else:
                require(select.select([root_fd], [], [], 5)[0], "root survived observer exit")
                if child_fd is not None:
                    require(select.select([child_fd], [], [], 5)[0], "backend survived observer exit")
            log.flush()
            content = open(log_path).read()
            if command in ("exec", "execat", "thread-exec"):
                require("REJECT root-exec" in content, "wrong rejection cause")
            if name == "untraced-disabled":
                require("REJECT root-exec" not in content, "escape was actually observed")
            return {"case": name, "result": "PASS", "exit_code": code, "log": content}
        finally:
            for fd in (root_fd, child_fd):
                if fd is not None:
                    if not select.select([fd], [], [], 0)[0]:
                        signal.pidfd_send_signal(fd, signal.SIGKILL)
                        require(select.select([fd], [], [], 5)[0], "cleanup did not terminate target")
                    os.close(fd)
            if process.poll() is None:
                process.kill()
            process.wait(timeout=10)
            process.stdin.close()
            process.stdout.close()


def main():
    before = len(os.listdir("/proc/self/fd"))
    results = [
        case("normal-exit-with-child"),
        case("exec", "exec"),
        case("execat", "execat"),
        case("thread-exec", "thread-exec"),
        case("untraced-exec", "untraced-exec"),
        case("clone3", "clone3"),
        case("observer-crash", crash=True),
        case("observer-crash-without-exitkill", crash=True, observer="/exec-observer-no-exitkill"),
        case("untraced-disabled", "untraced-exec", observer="/exec-observer-disabled"),
    ]
    started = time.monotonic()
    go = subprocess.run(["/exec-observer", "65534", "65534", "/go-probe"], capture_output=True, timeout=30)
    require(go.returncode == 0 and go.stdout == b"GO_PASS threads=24 subprocesses=10\n", "Go compatibility failed: " + go.stderr.decode())
    results.append({"case": "go-threads-and-backends", "result": "PASS", "exit_code": go.returncode,
                    "elapsed_seconds": time.monotonic() - started, "stdout": go.stdout.decode(), "log": go.stderr.decode()})
    require(len(os.listdir("/proc/self/fd")) == before, "observer test leaked FDs")
    print(json.dumps({"schema_version": 1, "kernel": os.uname().release,
                      "architecture": os.uname().machine, "fd_delta": 0, "results": results,
                      "production_authority": False}, indent=2))


if __name__ == "__main__":
    main()
