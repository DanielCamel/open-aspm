package version

import "fmt"

var (
	// These values are overridden by release builds through -ldflags.
	Version = "dev"
	Commit  = "unknown"
	Date    = "unknown"
)

// Info describes one Open ASPM build.
type Info struct {
	Version string
	Commit  string
	Date    string
}

// Current returns the build information compiled into the process.
func Current() Info {
	return Info{
		Version: Version,
		Commit:  Commit,
		Date:    Date,
	}
}

// String returns a stable human-readable representation.
func (info Info) String() string {
	return fmt.Sprintf("open-aspm %s (commit=%s, date=%s)", info.Version, info.Commit, info.Date)
}
