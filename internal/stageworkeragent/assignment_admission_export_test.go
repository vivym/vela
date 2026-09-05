package stageworkeragent

import "os"

// SetAssignmentAdmissionSyncHookForTest injects the post-Rename durability boundary.
func SetAssignmentAdmissionSyncHookForTest(gate *FileAssignmentAdmission, hook func(func() error) error) func() {
	gate.mu.Lock()
	original := gate.files.syncDirectory
	gate.files.syncDirectory = func(root *os.Root) error { return hook(func() error { return original(root) }) }
	gate.mu.Unlock()
	return func() {
		gate.mu.Lock()
		defer gate.mu.Unlock()
		gate.files.syncDirectory = original
	}
}
