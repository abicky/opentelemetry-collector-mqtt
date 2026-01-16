package mqttreceiver

import (
	"context"

	"github.com/abicky/opentelemetry-collector-mqtt/receiver/mqttreceiver/internal/metadata"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
)

// NewFactory creates a factory for mqttreceiver.
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		metadata.Type,
		createDefaultConfig,
		receiver.WithLogs(createLogsReceiver, metadata.LogsStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{}
}

func createLogsReceiver(_ context.Context, params receiver.Settings, baseCfg component.Config, consumer consumer.Logs) (receiver.Logs, error) {
	return newLogsReceiver(baseCfg.(*Config), params.Logger, consumer)
}
