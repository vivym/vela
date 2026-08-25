package workerrecovery

import (
	"errors"
	"path/filepath"
)

type XFSProjectQuotaConfig struct {
	DevicePath string
	ProjectID  uint32
}

type XFSProjectQuotaObservation struct {
	Space
	RootDevice uint64
	RootInode  uint64
}

func ProbeXFSProjectQuota(
	root string,
	config XFSProjectQuotaConfig,
) (XFSProjectQuotaObservation, error) {
	root = filepath.Clean(root)
	devicePath := filepath.Clean(config.DevicePath)
	if !filepath.IsAbs(root) || root == string(filepath.Separator) ||
		!filepath.IsAbs(devicePath) || devicePath == string(filepath.Separator) || config.ProjectID == 0 {
		return XFSProjectQuotaObservation{}, errors.New("XFS project quota requires absolute root and device paths and a positive project ID")
	}
	return probeXFSProjectQuota(root, XFSProjectQuotaConfig{
		DevicePath: devicePath,
		ProjectID:  config.ProjectID,
	})
}
