package input

// GCPCloudStorageConfig contains configuration fields for the Google Cloud
// Storage input type.
type GCPCloudStorageConfig struct {
	Bucket string `json:"bucket" yaml:"bucket"`
	Codec  string `json:"codec" yaml:"codec"`
}

// InputGCPCloudStorageConfig contains configuration fields for the Google Cloud
// Storage input type.
type InputGCPCloudStorageConfig struct {
	GCPCloudStorageConfig
	Prefix        string `json:"prefix" yaml:"prefix"`
	DeleteObjects bool   `json:"delete_objects" yaml:"delete_objects"`
}

// ProcessorGCPCloudStorageConfig contains configuration fields for the Google Cloud
// Storage processor type.
type ProcessorGCPCloudStorageConfig struct {
	GCPCloudStorageConfig
	Action string `json:"action" yaml:"action"`
}

// NewGCPCloudStorageConfig creates a new GCPCloudStorageConfig with default
// values.
func NewGCPCloudStorageConfig() GCPCloudStorageConfig {
	return GCPCloudStorageConfig{
		Codec: "all-bytes",
	}
}

// NewInputGCPCloudStorageConfig creates a new InputGCPCloudStorageConfig with default
// values.
func NewInputGCPCloudStorageConfig() InputGCPCloudStorageConfig {
	return InputGCPCloudStorageConfig{
		GCPCloudStorageConfig: GCPCloudStorageConfig{
			Codec: "all-bytes",
		},
	}
}
