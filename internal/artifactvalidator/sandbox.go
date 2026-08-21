package artifactvalidator

type SandboxConfig struct {
	HelperPath     string
	FFprobePath    string
	RootDirectory  string
	MaxOutputBytes int64
	MaxStderrBytes int64
}
