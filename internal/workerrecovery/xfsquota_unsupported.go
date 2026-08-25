//go:build !linux

package workerrecovery

import "errors"

func probeXFSProjectQuota(
	string,
	XFSProjectQuotaConfig,
) (XFSProjectQuotaObservation, error) {
	return XFSProjectQuotaObservation{}, errors.New("XFS project quota probing is supported only on Linux")
}
