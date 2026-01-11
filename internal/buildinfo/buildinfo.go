package buildinfo

// These variables are set at build time using -ldflags
var (
	// BuildID is a unique identifier for this build
	BuildID = "unknown"
	// BuildTime is the build timestamp
	BuildTime = "unknown"
)
