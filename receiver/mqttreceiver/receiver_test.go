package mqttreceiver

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/eclipse/paho.mqtt.golang"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/golden"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/plogtest"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

type configModifier func(*Config)

func (m configModifier) apply(cfg *Config) {
	m(cfg)
}

type logModifier func(plog.Logs)

func (m logModifier) apply(logs plog.Logs) {
	m(logs)
}

func Test_logsReceiver(t *testing.T) {
	broker, err := mqttBrokerContainer.PortEndpoint(t.Context(), "1883", "tcp")
	require.NoError(t, err, "Failed to get port endpoint")

	publisher := mqtt.NewClient(mqtt.NewClientOptions().AddBroker(broker))
	if token := publisher.Connect(); token.Wait() {
		require.NoError(t, token.Error(), "Failed to connect to MQTT broker")
	}
	t.Cleanup(func() {
		publisher.Disconnect(1_000)
	})

	tests := []struct {
		name            string
		payload         string
		compareLogsOpts []plogtest.CompareLogsOption
		configModifier  configModifier
		logModifier     logModifier
	}{
		{
			name:            "Basic",
			payload:         "payload",
			compareLogsOpts: []plogtest.CompareLogsOption{plogtest.IgnoreObservedTimestamp(), plogtest.IgnoreTimestamp()},
			configModifier:  func(cfg *Config) {},
			logModifier:     func(logs plog.Logs) {},
		},
		{
			name:            "With username",
			payload:         "payload",
			compareLogsOpts: []plogtest.CompareLogsOption{plogtest.IgnoreObservedTimestamp(), plogtest.IgnoreTimestamp()},
			configModifier: func(cfg *Config) {
				cfg.Username = "username"
			},
			logModifier: func(logs plog.Logs) {
				logs.ResourceLogs().At(0).Resource().Attributes().PutStr("mqtt.username", "username")
			},
		},
		{
			name:            "With timestamp",
			payload:         `{"time":"2026-01-23T12:34:56+0000"}`,
			compareLogsOpts: []plogtest.CompareLogsOption{plogtest.IgnoreObservedTimestamp()},
			configModifier: func(cfg *Config) {
				cfg.Timestamp = `Time(ParseJSON(log.body.string)["time"], "%Y-%m-%dT%H:%M:%S%z")`
			},
			logModifier: func(logs plog.Logs) {
				ts, err := time.Parse("2006-01-02T15:04:05-0700", "2026-01-23T12:34:56+0000")
				require.NoError(t, err, "Failed to parse timestamp for test")
				logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).SetTimestamp(pcommon.NewTimestampFromTime(ts))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			topic := "test/topic/" + t.Name()
			cfg := createDefaultConfig().(*Config)
			cfg.Broker = broker
			cfg.Topics = []string{topic}
			tt.configModifier(cfg)

			sink := new(consumertest.LogsSink)
			receiver, err := newLogsReceiver(cfg, receivertest.NewNopSettings(component.MustNewType("mqtt")), sink)
			require.NoError(t, err, "Failed to create receiver")

			require.NoError(t, receiver.Start(t.Context(), componenttest.NewNopHost()), "Failed to start receiver")

			if token := publisher.Publish(topic, 1, false, tt.payload); token.Wait() {
				require.NoError(t, token.Error(), "Failed to publish message to MQTT broker")
			}

			require.Eventually(t, func() bool {
				return sink.LogRecordCount() == 1
			}, 5*time.Second, 50*time.Millisecond, "Expected to receive 1 log record but got %d", sink.LogRecordCount())

			require.NoError(t, receiver.Shutdown(t.Context()), "Failed to shutdown receiver")

			allLogs := sink.AllLogs()
			require.Len(t, allLogs, 1, "Expected to only receive 1 log but got %d", len(allLogs))

			expected, err := golden.ReadLogs(filepath.Join("testdata", "expected_base_logs.yaml"))
			require.NoError(t, err, "Failed to read expected logs")

			resourceLog := expected.ResourceLogs().At(0)
			resourceLog.Resource().Attributes().PutStr("mqtt.topic", topic)
			resourceLog.ScopeLogs().At(0).LogRecords().At(0).Body().SetStr(tt.payload)
			tt.logModifier.apply(expected)

			require.NoError(t, plogtest.CompareLogs(expected, allLogs[0], tt.compareLogsOpts...))
		})
	}
}
