package iotseed

import (
	"net/url"
)

type Config struct {
	// Whether Dingding integration is enabled.
	Enabled bool `toml:"enabled" override:"enabled"`
	// message type
	APIServer string `toml:"api-servers" override:"api-servers"`
	// access_token
	APIVersion string `toml:"api-version"  override:"api-version"`

	TerminalControllerUri string `toml:"terminal-controller-uri"  override:"terminal-controller-uri"`

	Username string `toml:"username" override:"username"`

	Password string `toml:"password" override:"password"`

	// Path to CA file
	SSLCA string `toml:"ssl-ca" override:"ssl-ca"`
	// Path to host cert file
	SSLCert string `toml:"ssl-cert" override:"ssl-cert"`
	// Path to cert key file
	SSLKey string `toml:"ssl-key" override:"ssl-key"`
	// Use SSL but skip chain & host verification
	InsecureSkipVerify bool `toml:"insecure-skip-verify" override:"insecure-skip-verify"`
}

func NewConfig() Config {
	return Config{
		Enabled:               false,
		APIServer:             "http://127.0.0.1:8000/api",
		APIVersion:            "v2",
		TerminalControllerUri: "terminal-devices",
		InsecureSkipVerify:    true,
	}
}

func (c Config) Validate() error {
	if !c.Enabled {
		return nil
	}

	_, err := url.Parse(c.APIServer)
	if err != nil {
		return err
	}

	return nil
}
