package mqttreceiver

import (
	"fmt"
	"net/url"
)

// Config represents the receiver config settings in a Collector configuration file
type Config struct {
	Broker   string   `mapstructure:"broker"`
	Username string   `mapstructure:"username"`
	Password string   `mapstructure:"password"`
	Topics   []string `mapstructure:"topics"`
}

// Validate checks if the receiver configuration is valid
func (cfg *Config) Validate() error {
	u, err := url.Parse(cfg.Broker)
	if err != nil {
		return fmt.Errorf("invalid broker URL %q: %w", cfg.Broker, err)
	}

	if u.Scheme != "tcp" {
		return fmt.Errorf("invalid broker URL %q: only tcp scheme is supported and it must start with \"tcp://\"", cfg.Broker)
	}

	if u.Port() == "" {
		return fmt.Errorf("invalid broker URL %q: missing port number", cfg.Broker)
	}

	if len(cfg.Topics) == 0 {
		return fmt.Errorf("at least one topic must be set")
	}

	return nil
}
