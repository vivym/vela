"""Falsify retained proc mem readability as proof of no intervening exec.

Run only in the disposable runner image, as root with CAP_SYS_PTRACE. The target
has no capabilities, UID/GID 65534, no_new_privs and dumpability disabled. No Vela
API or grant is exercised. Failures are fatal; unsupported kernels do not skip.
"""

import hashlib
import json
import os
import select
import signal
import subprocess


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def line(process):
    ready, _, _ = select.select([process.stdout], [], [], 5)
    require(ready, "target event timed out")
    result = process.stdout.readline().decode().strip()
    require(result, "target exited before event")
    return result


def command(process, value):
    process.stdin.write((value + "\n").encode())
    process.stdin.flush()


def exited(pidfd):
    return bool(select.select([pidfd], [], [], 0)[0])


def await_exit(pidfd):
    require(select.select([pidfd], [], [], 5)[0], "process did not exit")


def snapshot(root):
    def read(name):
        fd = os.open(name, os.O_RDONLY | os.O_CLOEXEC, dir_fd=root)
        try:
            with os.fdopen(fd, "rb", closefd=False) as stream:
                return stream.read()
        finally:
            os.close(fd)

    stat = read("stat").rsplit(b")", 1)[1].split()
    status = dict(row.split(b":", 1) for row in read("status").splitlines())
    executable = os.stat("exe", dir_fd=root)
    return {
        "start_ticks": int(stat[19]),  # field 22; list starts at field 3
        "uid": status[b"Uid"].decode().split(),
        "gid": status[b"Gid"].decode().split(),
        "threads": int(status[b"Threads"]),
        "no_new_privs": int(status[b"NoNewPrivs"]),
        "effective_capabilities": status[b"CapEff"].strip().decode(),
        "argv_sha256": hashlib.sha256(read("cmdline")).hexdigest(),
        "environ_sha256": hashlib.sha256(read("environ")).hexdigest(),
        "executable_device": executable.st_dev,
        "executable_inode": executable.st_ino,
    }


def case(name, holder_kind=None, sequence=(), owner_exit=False):
    process = subprocess.Popen(
        ["/probe-a", "target"], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        bufsize=0, env={"PATH": "/usr/bin:/bin", "HOME": "/"},
        user=65534, group=65534, extra_groups=[],
    )
    pidfd = root = retained = holder_fd = None
    holder = None
    try:
        require(line(process) == f"READY A {process.pid}", "unexpected initial image")
        pidfd = os.pidfd_open(process.pid)
        root = os.open(f"/proc/{process.pid}", os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
        if holder_kind:
            command(process, holder_kind)
            event = line(process).split()
            require(len(event) == 2 and event[0] == "HOLDER", "missing holder")
            holder = int(event[1])
            holder_fd = os.pidfd_open(holder)
        initial = snapshot(root)
        require(initial["uid"] == ["65534"] * 4, "target must be non-root")
        require(initial["gid"] == ["65534"] * 4, "unexpected target group")
        require(initial["effective_capabilities"] == "0000000000000000", "target has capabilities")
        require(initial["no_new_privs"] == 1 and initial["threads"] == 1, "unexpected target state")

        # Derive a readable address from the retained proc directory, not target
        # stdout. The first file mapping contains the ELF header in this fixture.
        maps_fd = os.open("maps", os.O_RDONLY | os.O_CLOEXEC, dir_fd=root)
        with os.fdopen(maps_fd, "rb") as stream:
            maps = stream.read().decode().splitlines()
        mapping = next(row.split() for row in maps
                       if row.split()[-1] == "/probe-a" and row.split()[2] == "00000000")
        require(mapping[1].startswith("r"), "address is not readable")
        address = int(mapping[0].split("-")[0], 16)
        retained = os.open("mem", os.O_RDONLY | os.O_CLOEXEC, dir_fd=root)
        original = os.pread(retained, 4, address)
        require(original == b"\x7fELF", "initial memory read did not reach ELF mapping")
        steps = []
        for executable in sequence:
            command(process, "exec-" + executable.lower())
            require(line(process) == f"READY {executable} {process.pid}", "exec not observed")
            observed = snapshot(root)
            require(not exited(pidfd), "original process unexpectedly exited")
            require(observed["start_ticks"] == initial["start_ticks"], "start ticks changed")
            for key in ("uid", "gid", "threads", "no_new_privs", "effective_capabilities", "argv_sha256", "environ_sha256"):
                require(observed[key] == initial[key], key + " changed across exec")
            same_inode = observed["executable_inode"] == initial["executable_inode"]
            require(same_inode == (executable == "A"), "independent exe measurement disagrees")
            data = os.pread(retained, 4, address)
            require(data == (original if holder_kind == "share" else b""), "unexpected old-mm read")
            steps.append({"image": executable, "pidfd_live": True,
                          "same_executable_inode": same_inode, "old_mm_hex": data.hex()})
        final = snapshot(root)
        if sequence and sequence[-1] == "A":
            require(final == initial, "ABA did not restore complete sampled identity")
        if owner_exit:
            command(process, "exit")
            require(process.wait(timeout=5) == 0, "target exit failed")
            await_exit(pidfd)
            require(os.pread(retained, 4, address) == original, "shared mm should outlive owner")
        if holder_fd is not None:
            require(not exited(holder_fd), "holder exited prematurely")
            signal.pidfd_send_signal(holder_fd, signal.SIGKILL)
            await_exit(holder_fd)
            if not owner_exit:
                command(process, "reap")
                require(line(process) == f"REAPED {holder}", "holder was not reaped")
            if sequence or owner_exit:
                require(os.pread(retained, 4, address) == b"", "old mm survived its last user")
        if not sequence and not owner_exit:
            require(os.pread(retained, 4, address) == original, "no-exec control lost memory")
        if process.poll() is None:
            command(process, "exit")
            require(process.wait(timeout=5) == 0, "target exit failed")
            await_exit(pidfd)
        require(os.pread(retained, 4, address) == b"", "old mm alive after all users exit")
        return {"case": name, "result": "PASS", "holder": holder_kind,
                "initial": initial, "steps": steps, "owner_exited_before_holder": owner_exit,
                "eof_after_last_mm_user": True}
    finally:
        if holder_fd is not None and not exited(holder_fd):
            signal.pidfd_send_signal(holder_fd, signal.SIGKILL)
            await_exit(holder_fd)
        if process.poll() is None:
            process.kill()
        process.wait(timeout=5)
        process.stdin.close()
        process.stdout.close()
        for fd in (retained, root, holder_fd, pidfd):
            if fd is not None:
                os.close(fd)


def main():
    initial_fds = len(os.listdir("/proc/self/fd"))
    results = [
        case("no-exec"),
        case("same-executable-exec", sequence=("A",)),
        case("aba-exec", sequence=("B", "A")),
        case("ordinary-fork-exec", holder_kind="fork", sequence=("A",)),
        case("shared-vm-exec", holder_kind="share", sequence=("A",)),
        case("shared-vm-aba", holder_kind="share", sequence=("B", "A")),
        case("shared-vm-owner-exit", holder_kind="share", owner_exit=True),
    ]
    require(len(os.listdir("/proc/self/fd")) == initial_fds, "probe leaked descriptors")
    print(json.dumps({"schema_version": 1, "kernel": os.uname().release,
                      "architecture": os.uname().machine, "results": results,
                      "fd_delta": 0, "retained_mem_is_exec_authority": False}, indent=2))


if __name__ == "__main__":
    main()
