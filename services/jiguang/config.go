package jiguang

import (
	"net/url"

	"github.com/pkg/errors"
)

// DefaultPushoverURL is the default URL for the Jiguang API.
const (
	DefaultJiguangURL = "https://api.jpush.cn/v3/push"
)

// Config is the [jiguang] configuration as defined in the Kapacitor configuration file.
type Config struct {
	// Whether Jiguang integration is enabled.
	Enabled bool `toml:"enabled" override:"enabled"`
	// The Jiguang API token.
	APIKey string `toml:"api-key" override:"api-key"`
	APISecret string `toml:"api-secret" override:"api-secret"`
	// The User/Group that will be alerted.
	// The URL for the Jiguang API.
	// Default: DefaultPushoverAPI
	URL string `toml:"url" override:"url"`
	// 制定为生产环境还是测试环境: prod or sandbox, 默认为sandbox
	ENV string `toml:"env" override:"env"`
}

// NewConfig returns a new Jiguang configuration with the URL set to be
// the default jiguang URL.
func NewConfig() Config {
	return Config{
		Enabled:false,
		URL: DefaultJiguangURL,
		ENV: "sandbox",
	}
}

// Validate ensures that all configuration options are valid. The
// Token, User, and URL parameters must be specified to be considered
// valid.
func (c Config) Validate() error {
	if c.Enabled {
		if c.APIKey == "" {
			return errors.New("must specify APIKey")
		}

		if c.APISecret == "" {
			return errors.New("must specify APISecret")
		}

		if c.URL == "" {
			return errors.New("must specify url")
		}
		if _, err := url.Parse(c.URL); err != nil {
			return errors.Wrapf(err, "invalid URL %q", c.URL)
		}
	}

	return nil
}
