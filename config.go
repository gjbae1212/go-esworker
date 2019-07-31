package esworker

import "net/http"

const (
	V5 ESVersion = iota
	V6
	V7
)

// config is environment variable, which is used to dispatcher.
type config struct {
	version    ESVersion         // elastic search version
	addrs      []string          // a list of elastic search nodes to use.
	username   string            // username for http basic authentication.
	password   string            // password for http basic authentication.
	cloudId    string            // endpoint for elastic cloud.
	apiKey     string            // base64-encoded token for authorization.
	transport  http.RoundTripper // http transport object.
	logger     *Logger           // intermediate logger.
	queueSize  int               // as queue size, it is the maximum value that could store an action.
	workerSize int               // worker size to currently run to process an action.
}

// Option is something for dependency injection.
type Option interface {
	apply(cfg *config)
}

// OptionFunc is a practical struct for Option.
type OptionFunc func(cfg *config)

func (of OptionFunc) apply(cfg *config) { of(cfg) }

// ESVersion would identify a version about elastic search.
type ESVersion int

func (v ESVersion) GetString() string {
	switch v {
	case V5:
		return "ES5.X"
	case V6:
		return "ES6.X"
	case V7:
		return "ES7.X"
	default:
		return "unknown"
	}
}

// WithESVersionOption has associated version that elastic search nodes are running
func WithESVersionOption(v ESVersion) OptionFunc {
	return func(cfg *config) {
		cfg.version = v
	}
}

// WithAddressesOption has associated a list of elastic search nodes
func WithAddressesOption(addrs []string) OptionFunc {
	return func(cfg *config) {
		cfg.addrs = addrs
	}
}

// WithUsernameOption has associated username for HTTP basic authentication.
func WithUsernameOption(username string) OptionFunc {
	return func(cfg *config) {
		cfg.username = username
	}
}

// WithPasswordOption has associated password for HTTP basic authentication.
func WithPasswordOption(password string) OptionFunc {
	return func(cfg *config) {
		cfg.password = password
	}
}

// WithCloudIdOption has associated cloud-id about endpoint for elastic cloud
func WithCloudIdOption(id string) OptionFunc {
	return func(cfg *config) {
		cfg.cloudId = id
	}
}

// WithApiKeyOption has associated apikey for authorization.
func WithApiKeyOption(key string) OptionFunc {
	return func(cfg *config) {
		cfg.apiKey = key
	}
}

// WithTransportOption has associated http transport object.
func WithTransportOption(tp http.RoundTripper) OptionFunc {
	return func(cfg *config) {
		cfg.transport = tp
	}
}

// WithLoggerOption has associated logger object.
func WithLoggerOption(logger *Logger) OptionFunc {
	return func(cfg *config) {
		cfg.logger = logger
	}
}

// WithQueueSizeOption has associated queue size.
func WithQueueSizeOption(size int) OptionFunc {
	return func(cfg *config) {
		cfg.queueSize = size
	}
}

// WithWorkerSizeOption has asscoiated worker size.
func WithWorkerSizeOption(size int) OptionFunc {
	return func(cfg *config) {
		cfg.workerSize = size
	}
}
