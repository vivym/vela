package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

type runtimeMemoryProbe struct {
	MemoryOpen bool
	Attach     bool
	Dumpable   int
}

func TestRuntimeProcessProtection(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("run the memory isolation test as non-root without CAP_SYS_PTRACE")
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary, "-test.run=^TestRuntimeProcessProtectionHelper$")
	command.Env = []string{"VELA_RUNTIME_PROTECTION_HELPER=victim"}
	input, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	output, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	t.Cleanup(func() {
		if !waited {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
		_ = input.Close()
		_ = output.Close()
	})
	decoder := json.NewDecoder(output)
	for _, phase := range []string{"baseline", "protect", "repeat"} {
		if _, err := fmt.Fprintln(input, phase); err != nil {
			t.Fatal(err)
		}
		var probe runtimeMemoryProbe
		if err := decoder.Decode(&probe); err != nil {
			detail, _ := io.ReadAll(io.LimitReader(io.MultiReader(decoder.Buffered(), output), 16<<10))
			t.Fatalf("victim failed at %s: %v\n%s", phase, err, detail)
		}
		if phase == "baseline" {
			if !probe.MemoryOpen || !probe.Attach || probe.Dumpable != 1 {
				t.Fatalf("kernel/environment did not expose baseline same-UID access: %+v", probe)
			}
		} else if probe.MemoryOpen || probe.Attach || probe.Dumpable != 0 {
			t.Fatalf("protected Runtime exposed memory after backend exec: %+v", probe)
		}
		t.Logf("%s: same-UID exec child memory-open=%v ptrace-attach=%v parent-dumpable=%d", phase, probe.MemoryOpen, probe.Attach, probe.Dumpable)
	}
	if err := input.Close(); err != nil {
		t.Fatal(err)
	}
	err = command.Wait()
	waited = true
	if err != nil {
		t.Fatalf("victim: %v %s", err, stderr.String())
	}
}

func TestRuntimeProcessProtectionHelper(t *testing.T) {
	mode := os.Getenv("VELA_RUNTIME_PROTECTION_HELPER")
	if mode == "" {
		t.Skip("isolated process helper")
	}
	if mode == "deny-set" || mode == "deny-get" {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		operation := uint32(unix.PR_SET_DUMPABLE)
		if mode == "deny-get" {
			operation = unix.PR_GET_DUMPABLE
		}
		filter := []unix.SockFilter{
			{Code: unix.BPF_LD | unix.BPF_W | unix.BPF_ABS, K: 0},
			{Code: unix.BPF_JMP | unix.BPF_JEQ | unix.BPF_K, K: uint32(unix.SYS_PRCTL), Jf: 3},
			{Code: unix.BPF_LD | unix.BPF_W | unix.BPF_ABS, K: 16},
			{Code: unix.BPF_JMP | unix.BPF_JEQ | unix.BPF_K, K: operation, Jf: 1},
			{Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_ERRNO | uint32(unix.EPERM)},
			{Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_ALLOW},
		}
		program := unix.SockFprog{Len: uint16(len(filter)), Filter: &filter[0]}
		if err := unix.Prctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0); err != nil {
			t.Fatal(err)
		}
		_, _, errno := unix.Syscall6(unix.SYS_PRCTL, unix.PR_SET_SECCOMP, unix.SECCOMP_MODE_FILTER, uintptr(unsafe.Pointer(&program)), 0, 0, 0)
		runtime.KeepAlive(filter)
		if errno != 0 {
			t.Fatal(errno)
		}
		if err := run(t.Context()); !errors.Is(err, unix.EPERM) {
			t.Fatalf("kernel protection failure did not stop actual entry: %v", err)
		}
		return
	}
	if mode == "attacker" {
		pid, err := strconv.Atoi(os.Getenv("VELA_RUNTIME_PROTECTION_TARGET"))
		if err != nil || pid <= 0 {
			t.Fatal("invalid victim pid")
		}
		probe := runtimeMemoryProbe{}
		file, err := os.OpenFile(fmt.Sprintf("/proc/%d/mem", pid), os.O_RDWR, 0)
		if err == nil {
			probe.MemoryOpen = true
			_ = file.Close()
		} else if !errors.Is(err, os.ErrPermission) {
			t.Fatalf("unexpected memory error: %v", err)
		}
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		err = unix.PtraceAttach(pid)
		if err == nil {
			probe.Attach = true
			var status unix.WaitStatus
			if _, err := unix.Wait4(pid, &status, unix.WALL, nil); err != nil {
				t.Fatal(err)
			}
			if err := unix.PtraceDetach(pid); err != nil {
				t.Fatal(err)
			}
		} else if !errors.Is(err, unix.EPERM) {
			t.Fatalf("unexpected ptrace error: %v", err)
		}
		if err := json.NewEncoder(os.Stdout).Encode(probe); err != nil {
			t.Fatal(err)
		}
		return
	}
	if mode != "victim" {
		t.Fatal("unknown helper mode")
	}
	// Relax Yama only in this victim fixture so a passing baseline proves
	// dumpability is the changed enforcement mechanism, not ambient policy.
	// EINVAL means this kernel has no Yama ptracer option; the successful
	// baseline is still mandatory and independently checks available access.
	if err := unix.Prctl(unix.PR_SET_PTRACER, unix.PR_SET_PTRACER_ANY, 0, 0, 0); err != nil && !errors.Is(err, unix.EINVAL) {
		t.Fatal(err)
	}
	if err := unix.Prctl(unix.PR_SET_DUMPABLE, 1, 0, 0, 0); err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		phase := scanner.Text()
		if phase != "baseline" {
			// Invoke the actual production entry; invalid configuration must fail
			// only after installing process protection, before any backend exists.
			if err := run(t.Context()); err == nil {
				t.Fatal("missing launch configuration unexpectedly started Runtime")
			}
		}
		binary, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		child := exec.CommandContext(t.Context(), binary, "-test.run=^TestRuntimeProcessProtectionHelper$")
		child.Env = []string{"VELA_RUNTIME_PROTECTION_HELPER=attacker", "VELA_RUNTIME_PROTECTION_TARGET=" + strconv.Itoa(os.Getpid())}
		result, err := child.CombinedOutput()
		if err != nil {
			t.Fatalf("same-UID backend probe: %v %s", err, result)
		}
		var probe runtimeMemoryProbe
		if err := json.NewDecoder(strings.NewReader(string(result))).Decode(&probe); err != nil {
			t.Fatal(err)
		}
		probe.Dumpable, err = unix.PrctlRetInt(unix.PR_GET_DUMPABLE, 0, 0, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.NewEncoder(os.Stdout).Encode(probe); err != nil {
			t.Fatal(err)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeProcessProtectionFailsClosed(t *testing.T) {
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"deny-set", "deny-get"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, binary, "-test.run=^TestRuntimeProcessProtectionHelper$")
			command.Env = []string{"VELA_RUNTIME_PROTECTION_HELPER=" + mode}
			output, err := command.CombinedOutput()
			if err != nil || !bytes.Contains(output, []byte("PASS")) {
				t.Fatalf("failed protection was not enforced: %v %s", err, output)
			}
		})
	}
}
