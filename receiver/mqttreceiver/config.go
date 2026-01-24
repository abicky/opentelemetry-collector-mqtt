package mqttreceiver

import (
	"fmt"
	"net/url"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottllog"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/ottlfuncs"
	"go.opentelemetry.io/collector/component"
	"go.uber.org/zap"
)

// Config represents the receiver config settings in a Collector configuration file
type Config struct {
	Broker    string   `mapstructure:"broker"`
	Username  string   `mapstructure:"username"`
	Password  string   `mapstructure:"password"`
	Topics    []string `mapstructure:"topics"`
	Timestamp string   `mapstructure:"timestamp"`
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

	if _, err := cfg.parseTimestampExpression(component.TelemetrySettings{Logger: zap.NewNop()}); err != nil {
		return err
	}

	return nil
}

func (cfg *Config) parseTimestampExpression(settings component.TelemetrySettings) (*ottl.ValueExpression[*ottllog.TransformContext], error) {
	if cfg.Timestamp == "" {
		return nil, nil
	}

	parser, err := ottllog.NewParser(
		ottlfuncs.StandardFuncs[*ottllog.TransformContext](),
		settings,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create timestamp expression OTTL parser: %w", err)
	}

	valueExp, err := parser.ParseValueExpression(cfg.Timestamp)
	if err != nil {
		return nil, fmt.Errorf("invalid timestamp expression: %w", err)
	}

	return valueExp, nil
}
