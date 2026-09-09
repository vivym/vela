package nodeagent

import (
	"bytes"
	"os"
	"testing"

	"github.com/vivym/vela/internal/runtimechannel"
	"golang.org/x/sys/unix"
)

func TestRuntimeObserverProcessOfferDescriptors(t *testing.T) {
	for _, scenario := range []string{"valid", "missing", "regular-file", "duplicate", "truncated-rights", "oversized", "legacy-rejects-rights"} {
		t.Run(scenario, func(t *testing.T) {
			node, sender := runtimeObserverSocketpair(t)
			if err := configureRuntimeObserverChannel(node); err != nil {
				t.Fatal(err)
			}
			pidfd, err := unix.PidfdOpen(os.Getpid(), 0)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = unix.Close(pidfd) }()
			regular, err := os.Open("/proc/self/status")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = regular.Close() }()
			rights := []int{pidfd}
			wire := bytes.Repeat([]byte{'x'}, runtimeObserverFrameSize)
			switch scenario {
			case "missing":
				rights = nil
			case "regular-file":
				rights = []int{int(regular.Fd())}
			case "duplicate":
				rights = []int{pidfd, pidfd}
			case "truncated-rights":
				rights = make([]int, 250)
				for i := range rights {
					rights[i] = pidfd
				}
			case "oversized":
				wire = append(wire, 'x')
			}
			var ancillary []byte
			if len(rights) != 0 {
				ancillary = unix.UnixRights(rights...)
			}
			before := runtimeCallerDescriptorCount(t)
			if err := unix.Sendmsg(int(sender.Fd()), wire, ancillary, nil, 0); err != nil {
				t.Fatal(err)
			}
			if scenario == "legacy-rejects-rights" {
				if packet, _, message, err := runtimechannel.ReadPacket(node, runtimeObserverFrameSize); err == nil || packet != nil || message != -1 {
					t.Fatalf("ordinary protocol accepted descriptor offer: %v", err)
				}
			} else {
				packet, credentials, message, target, err := runtimechannel.ReadProcessOffer(node, runtimeObserverFrameSize)
				if scenario == "valid" {
					if err != nil || !bytes.Equal(packet, wire) || credentials.Pid != int32(os.Getpid()) {
						t.Fatalf("kernel process offer failed: %v", err)
					}
					if err := runtimechannel.SameLiveProcess(target, pidfd); err != nil {
						t.Fatal(err)
					}
					_ = unix.Close(message)
					_ = unix.Close(target)
				} else if err == nil || packet != nil || message != -1 || target != -1 {
					t.Fatalf("invalid offer escaped: message=%d target=%d err=%v", message, target, err)
				}
			}
			if after := runtimeCallerDescriptorCount(t); after != before {
				t.Fatalf("offer leaked descriptors: before=%d after=%d", before, after)
			}
		})
	}
}
