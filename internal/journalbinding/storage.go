package journalbinding

// FileIdentity is a local filesystem observation, not a signed Registry claim
// or a globally unique storage identity across remounts and inode reuse.
type FileIdentity struct {
	Device uint64 `json:"device"`
	Inode  uint64 `json:"inode"`
}

// StorageIdentity describes the actual directory and lock held during local
// preparation. Registry binding schema 1 does not sign these fields.
type StorageIdentity struct {
	Root FileIdentity `json:"root"`
	Lock FileIdentity `json:"lock"`
}

func (identity StorageIdentity) Valid() bool {
	return identity.Root.Inode != 0 && identity.Lock.Inode != 0 && identity.Root != identity.Lock
}
