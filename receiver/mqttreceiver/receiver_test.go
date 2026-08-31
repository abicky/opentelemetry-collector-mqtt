package mqttreceiver

import (
	"errors"
	"net"
	"path/filepath"
	"testing"
	"time"

	toxiproxy "github.com/Shopify/toxiproxy/v2/client"
	"github.com/eclipse/paho.mqtt.golang"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/golden"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/plogtest"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tctoxiproxy "github.com/testcontainers/testcontainers-go/modules/toxiproxy"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.uber.org/zap"
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

	publisher := newClient(t, broker)

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
				cfg.Password = "password"
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

func Test_logsReceiverResubscribesAfterReconnect(t *testing.T) {
	// Toxiproxy assigns the first proxy created with WithProxy to port 8666.
	const proxyPort = 8666

	toxiproxyContainer, err := tctoxiproxy.Run(
		t.Context(),
		"ghcr.io/shopify/toxiproxy:2.12.0",
		tctoxiproxy.WithProxy("mqtt", net.JoinHostPort(testcontainers.HostInternal, "1883")),
		testcontainers.WithHostPortAccess(1883),
	)
	testcontainers.CleanupContainer(t, toxiproxyContainer)
	require.NoError(t, err, "Failed to start Toxiproxy container")

	proxyHost, proxyPortString, err := toxiproxyContainer.ProxiedEndpoint(proxyPort)
	require.NoError(t, err, "Failed to get Toxiproxy endpoint")
	toxiproxyURI, err := toxiproxyContainer.URI(t.Context())
	require.NoError(t, err, "Failed to get Toxiproxy URI")
	proxy, err := toxiproxy.NewClient(toxiproxyURI).Proxy("mqtt")
	require.NoError(t, err, "Failed to get MQTT proxy")

	broker := "tcp://" + net.JoinHostPort(proxyHost, proxyPortString)
	publisher := newClient(t, broker)

	topic := "test/topic/" + t.Name()
	cfg := createDefaultConfig().(*Config)
	cfg.Broker = broker
	cfg.Topics = []string{topic}

	sink := new(consumertest.LogsSink)
	receiver, err := newLogsReceiver(cfg, receivertest.NewNopSettings(component.MustNewType("mqtt")), sink)
	require.NoError(t, err, "Failed to create receiver")
	require.NoError(t, receiver.Start(t.Context(), componenttest.NewNopHost()), "Failed to start receiver")
	t.Cleanup(func() {
		require.NoError(t, receiver.Shutdown(t.Context()), "Failed to shutdown receiver")
	})

	if token := publisher.Publish(topic, 1, false, "before reconnect"); token.Wait() {
		require.NoError(t, token.Error(), "Failed to publish message to MQTT broker")
	}

	require.Eventually(t, func() bool {
		return sink.LogRecordCount() == 1
	}, 5*time.Second, 50*time.Millisecond, "Expected to receive the message before reconnecting")

	// Emulate connection disruption
	require.NoError(t, proxy.Disable(), "Failed to disable MQTT proxy")
	require.Eventually(t, func() bool {
		return !publisher.IsConnectionOpen()
	}, 5*time.Second, 50*time.Millisecond, "Expected to disconnect from the broker")
	require.NoError(t, proxy.Enable(), "Failed to enable MQTT proxy")

	require.Eventually(t, func() bool {
		return publisher.IsConnectionOpen()
	}, 5*time.Second, 50*time.Millisecond, "Expected to reconnect to the broker")

	// The publisher may reconnect before the receiver has restored its subscription.
	// Retry because a non-retained message published during that gap is discarded.
	require.Eventually(t, func() bool {
		token := publisher.Publish(topic, 1, false, "after reconnect")
		if !token.WaitTimeout(time.Second) || token.Error() != nil {
			return false
		}
		return sink.LogRecordCount() >= 2
	}, 5*time.Second, 50*time.Millisecond, "Expected receiver to get a message after reconnecting")
}

func Test_logsReceiver_retrySubscriptions(t *testing.T) {
	subscriptionErrors := map[string][]error{
		"topic/one":   nil,
		"topic/two":   {errors.New("first attempt failed")},
		"topic/three": {errors.New("first attempt failed"), errors.New("second attempt failed")},
	}

	var subscriptionCalls []string
	client := &mqttClientStub{
		subscribe: func(topic string, _ byte, _ mqtt.MessageHandler) mqtt.Token {
			subscriptionCalls = append(subscriptionCalls, topic)
			errors := subscriptionErrors[topic]
			var tokenError error
			if len(errors) > 0 {
				tokenError = errors[0]
				subscriptionErrors[topic] = errors[1:]
			}
			return mqttTokenStub{
				waitTimeout: func(time.Duration) bool { return true },
				tokenError:  func() error { return tokenError },
				result:      map[string]byte{topic: 0},
			}
		},
	}
	receiver := &logsReceiver{
		config: &Config{Topics: []string{"topic/one", "topic/two", "topic/three"}},
		logger: zap.NewNop(),
	}

	receiver.retrySubscriptions(t.Context(), client, time.Nanosecond)

	require.Equal(t, []string{
		"topic/one",
		"topic/two",
		"topic/three",
		"topic/two",
		"topic/three",
		"topic/three",
	}, subscriptionCalls)
}

func Test_logsReceiver_subscribeTopic(t *testing.T) {
	broker, err := mqttBrokerContainer.PortEndpoint(t.Context(), "1883", "tcp")
	require.NoError(t, err, "Failed to get port endpoint")

	client := newClient(t, broker)

	receiver := &logsReceiver{
		logger: zap.NewNop(),
	}

	err = receiver.subscribeTopic(t.Context(), client, "test/topic/rejected")

	require.ErrorContains(t, err, "subscription rejected by broker")
}

func newClient(t *testing.T, broker string) mqtt.Client {
	t.Helper()

	client := mqtt.NewClient(mqtt.NewClientOptions().AddBroker(broker))
	if token := client.Connect(); token.Wait() {
		require.NoError(t, token.Error(), "Failed to connect to MQTT broker")
	}
	t.Cleanup(func() {
		client.Disconnect(1_000)
	})

	return client
}

type mqttClientStub struct {
	mqtt.Client
	subscribe func(string, byte, mqtt.MessageHandler) mqtt.Token
}

func (s *mqttClientStub) Subscribe(topic string, qos byte, callback mqtt.MessageHandler) mqtt.Token {
	return s.subscribe(topic, qos, callback)
}

type mqttTokenStub struct {
	mqtt.Token
	waitTimeout func(time.Duration) bool
	tokenError  func() error
	result      map[string]byte
}

var _ mqttSubscribeToken = (*mqttTokenStub)(nil)

func (s mqttTokenStub) WaitTimeout(timeout time.Duration) bool {
	return s.waitTimeout(timeout)
}

func (s mqttTokenStub) Error() error {
	return s.tokenError()
}

func (s mqttTokenStub) Result() map[string]byte {
	return s.result
}
