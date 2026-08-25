//go:build linux

package workerrecovery

import (
	"errors"
	"math"
	"path/filepath"
	"runtime"
	"unsafe"

	"golang.org/x/sys/unix"
)

const (
	xfsSuperMagic           = 0x58465342
	xfsIOCGetXAttr          = 0x801c581f
	xfsXFlagProjectInherit  = 0x00000200
	xfsProjectQuotaFlag     = 1 << 1
	xfsProjectQuotaCommand  = (((('X' << 8) + 3) << 8) | 2)
	xfsProjectStatusCommand = (((('X' << 8) + 8) << 8) | 2)
	xfsQuotaBasicBlockBytes = 512
	xfsDiskQuotaVersion     = 1
	xfsQuotaStatusVersion   = 1
	xfsProjectAccounting    = 1 << 4
	xfsProjectEnforcement   = 1 << 5
)

type xfsExtendedAttribute struct {
	XFlags         uint32
	ExtentSize     uint32
	Nextents       uint32
	ProjectID      uint32
	CopyExtentSize uint32
	Padding        [8]byte
}

type xfsDiskQuota struct {
	Version          int8
	Flags            int8
	FieldMask        uint16
	ID               uint32
	BlockHardLimit   uint64
	BlockSoftLimit   uint64
	InodeHardLimit   uint64
	InodeSoftLimit   uint64
	BlockCount       uint64
	InodeCount       uint64
	InodeTimer       int32
	BlockTimer       int32
	InodeWarnings    uint16
	BlockWarnings    uint16
	InodeTimerHigh   int8
	BlockTimerHigh   int8
	RTBlockTimerHigh int8
	Padding2         int8
	RTBlockHardLimit uint64
	RTBlockSoftLimit uint64
	RTBlockCount     uint64
	RTBlockTimer     int32
	RTBlockWarnings  uint16
	Padding3         int16
	Padding4         [8]byte
}

type xfsQuotaFileStatV struct {
	Inode       uint64
	BlockCount  uint64
	ExtentCount uint32
	Padding     uint32
}

type xfsQuotaStatV struct {
	Version             int8
	Padding1            uint8
	Flags               uint16
	InCoreQuotaCount    uint32
	UserQuota           xfsQuotaFileStatV
	GroupQuota          xfsQuotaFileStatV
	ProjectQuota        xfsQuotaFileStatV
	BlockTimeLimit      int32
	InodeTimeLimit      int32
	RTBlockTimeLimit    int32
	BlockWarningLimit   uint16
	InodeWarningLimit   uint16
	RTBlockWarningLimit uint16
	Padding3            uint16
	Padding4            uint32
	Padding2            [7]uint64
}

var (
	_ [28]byte  = [unsafe.Sizeof(xfsExtendedAttribute{})]byte{}
	_ [112]byte = [unsafe.Sizeof(xfsDiskQuota{})]byte{}
	_ [160]byte = [unsafe.Sizeof(xfsQuotaStatV{})]byte{}
)

func probeXFSProjectQuota(
	root string,
	config XFSProjectQuotaConfig,
) (XFSProjectQuotaObservation, error) {
	root = filepath.Clean(root)
	if !filepath.IsAbs(root) || root == string(filepath.Separator) {
		return XFSProjectQuotaObservation{}, errors.New("XFS project quota root must be an absolute non-root path")
	}
	rootFD, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return XFSProjectQuotaObservation{}, errors.New("XFS project quota root must be a pinned directory")
	}
	defer func() { _ = unix.Close(rootFD) }()
	var rootStat unix.Stat_t
	if err := unix.Fstat(rootFD, &rootStat); err != nil || rootStat.Ino == 0 {
		return XFSProjectQuotaObservation{}, errors.New("XFS project quota root has no Linux stat identity")
	}
	var filesystem unix.Statfs_t
	if err := unix.Fstatfs(rootFD, &filesystem); err != nil || uint64(filesystem.Type) != xfsSuperMagic {
		return XFSProjectQuotaObservation{}, errors.New("Worker scratch root is not on XFS")
	}
	var deviceStat unix.Stat_t
	if err := unix.Lstat(config.DevicePath, &deviceStat); err != nil ||
		deviceStat.Mode&unix.S_IFMT != unix.S_IFBLK {
		return XFSProjectQuotaObservation{}, errors.New("XFS quota device is not an exact block device")
	}
	if rootStat.Dev != deviceStat.Rdev {
		return XFSProjectQuotaObservation{}, errors.New("XFS quota device does not own the Worker scratch root")
	}

	var attribute xfsExtendedAttribute
	_, _, errno := unix.Syscall(
		unix.SYS_IOCTL,
		uintptr(rootFD),
		xfsIOCGetXAttr,
		uintptr(unsafe.Pointer(&attribute)),
	)
	if errno != 0 {
		return XFSProjectQuotaObservation{}, errno
	}
	if attribute.ProjectID != config.ProjectID || attribute.XFlags&xfsXFlagProjectInherit == 0 {
		return XFSProjectQuotaObservation{}, errors.New("Worker scratch root is not bound to the configured inherited XFS project")
	}

	device, err := unix.BytePtrFromString(config.DevicePath)
	if err != nil {
		return XFSProjectQuotaObservation{}, err
	}
	quotaState := xfsQuotaStatV{Version: xfsQuotaStatusVersion}
	_, _, errno = unix.Syscall6(
		unix.SYS_QUOTACTL,
		xfsProjectStatusCommand,
		uintptr(unsafe.Pointer(device)),
		0,
		uintptr(unsafe.Pointer(&quotaState)),
		0,
		0,
	)
	runtime.KeepAlive(device)
	runtime.KeepAlive(&quotaState)
	if errno != 0 {
		return XFSProjectQuotaObservation{}, errno
	}
	if quotaState.Version != xfsQuotaStatusVersion ||
		quotaState.Flags&(xfsProjectAccounting|xfsProjectEnforcement) !=
			xfsProjectAccounting|xfsProjectEnforcement {
		return XFSProjectQuotaObservation{}, errors.New("XFS project quota accounting and enforcement are not both enabled")
	}
	var quota xfsDiskQuota
	_, _, errno = unix.Syscall6(
		unix.SYS_QUOTACTL,
		xfsProjectQuotaCommand,
		uintptr(unsafe.Pointer(device)),
		uintptr(config.ProjectID),
		uintptr(unsafe.Pointer(&quota)),
		0,
		0,
	)
	runtime.KeepAlive(device)
	runtime.KeepAlive(&quota)
	if errno != 0 {
		return XFSProjectQuotaObservation{}, errno
	}
	if quota.Version != xfsDiskQuotaVersion || quota.ID != config.ProjectID ||
		uint8(quota.Flags)&xfsProjectQuotaFlag == 0 ||
		quota.BlockHardLimit == 0 || quota.BlockHardLimit > math.MaxInt64/xfsQuotaBasicBlockBytes {
		return XFSProjectQuotaObservation{}, errors.New("XFS project has no enforceable bounded hard quota")
	}
	total := int64(quota.BlockHardLimit * xfsQuotaBasicBlockBytes)
	usedBlocks := quota.BlockCount
	if usedBlocks > quota.BlockHardLimit {
		usedBlocks = quota.BlockHardLimit
	}
	used := int64(usedBlocks * xfsQuotaBasicBlockBytes)
	return XFSProjectQuotaObservation{
		Space:      Space{TotalBytes: total, FreeBytes: total - used},
		RootDevice: uint64(rootStat.Dev),
		RootInode:  rootStat.Ino,
	}, nil
}
