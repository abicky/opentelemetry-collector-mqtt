package mqttreceiver

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/eclipse/paho.mqtt.golang"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottllog"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/receiver"
	semconv "go.opentelemetry.io/otel/semconv/v1.38.0"
	"go.uber.org/zap"
)

const (
	tokenTimeout = 10 * time.Second
)

type logsReceiver struct {
	config       *Config
	brokerURL    *url.URL
	logger       *zap.Logger
	nextConsumer consumer.Logs
	cancel       context.CancelFunc
	timestampExp *ottl.ValueExpression[*ottllog.TransformContext]
}

func newLogsReceiver(cfg *Config, settings receiver.Settings, nextConsumer consumer.Logs) (*logsReceiver, error) {
	timestampExp, err := cfg.parseTimestampExpression(settings.TelemetrySettings)
	if err != nil {
		return nil, fmt.Errorf("failed to parse timestamp: %w", err)
	}

	return &logsReceiver{
		config:       cfg,
		logger:       settings.Logger,
		nextConsumer: nextConsumer,
		timestampExp: timestampExp,
	}, nil
}

func (lr *logsReceiver) Start(ctx context.Context, _ component.Host) error {
	lr.logger.Debug("Connect to the broker", zap.String("broker", lr.config.Broker), zap.String("username", lr.config.Username))

	opts := mqtt.NewClientOptions()

	opts.AddBroker(lr.config.Broker)
	opts.SetUsername(lr.config.Username)
	opts.SetPassword(lr.config.Password)

	lr.brokerURL = opts.Servers[0]

	client := mqtt.NewClient(opts)
	lr.cancel = func() {
		client.Disconnect(1_000)
		lr.logger.Debug("Disconnected the broker", zap.String("broker", lr.config.Broker))
	}

	if err := waitToken(ctx, client.Connect()); err != nil {
		lr.cancel()
		return fmt.Errorf("failed to connect to the broker: %w", err)
	}

	for _, topic := range lr.config.Topics {
		lr.logger.Debug("Subscribe to the topic", zap.String("topic", topic))
		if err := waitToken(ctx, client.Subscribe(topic, 0, lr.handleMessage)); err != nil {
			lr.cancel()
			return fmt.Errorf("failed to subscribe to the %q topic: %w", topic, err)
		}
	}

	return nil
}

func (lr *logsReceiver) Shutdown(_ context.Context) error {
	if lr.cancel != nil {
		lr.cancel()
	}
	return nil
}

func (lr *logsReceiver) handleMessage(client mqtt.Client, msg mqtt.Message) {
	lr.logger.Debug("Received message", zap.String("topic", msg.Topic()), zap.Uint16("message_id", msg.MessageID()))

	now := pcommon.NewTimestampFromTime(time.Now())

	// NOTE: Although plog.NewLogs says "This must be used only in testing code.",
	//   other receivers managed in the opentelemetry-collector-contrib use this function.
	logs := plog.NewLogs()

	reader := client.OptionsReader()
	brokerURL := reader.Servers()[0]

	resourceLogs := logs.ResourceLogs().AppendEmpty()

	resourceAttrs := resourceLogs.Resource().Attributes()
	resourceAttrs.PutStr(string(semconv.ServerAddressKey), brokerURL.Hostname())
	if brokerURL.Port() != "" {
		port, err := strconv.Atoi(brokerURL.Port())
		if err == nil {
			resourceAttrs.PutInt(string(semconv.ServerPortKey), int64(port))
		} else {
			lr.logger.Warn("Failed to convert the port to an integer", zap.Error(err), zap.String("port", brokerURL.Port()))
		}
	}
	resourceAttrs.PutStr(string(semconv.URLSchemeKey), brokerURL.Scheme)
	resourceAttrs.PutStr("mqtt.topic", msg.Topic())
	if lr.config.Username != "" {
		resourceAttrs.PutStr("mqtt.username", lr.config.Username)
	}

	scopeLogs := resourceLogs.ScopeLogs().AppendEmpty()
	logRecord := scopeLogs.LogRecords().AppendEmpty()
	logRecord.SetObservedTimestamp(now)
	logRecord.SetTimestamp(now)
	logRecord.Body().SetStr(string(msg.Payload()))
	logRecord.Attributes().PutStr(string(semconv.MessagingMessageIDKey), fmt.Sprint(msg.MessageID()))
	logRecord.Attributes().PutInt("mqtt.message.qos", int64(msg.Qos()))
	logRecord.Attributes().PutBool("mqtt.message.duplicate", msg.Duplicate())
	logRecord.Attributes().PutBool("mqtt.message.retained", msg.Retained())

	if lr.timestampExp != nil {
		tCtx := ottllog.NewTransformContextPtr(resourceLogs, scopeLogs, logRecord)
		timestamp, err := lr.evalTimestamp(tCtx)
		tCtx.Close()
		if err == nil {
			logRecord.SetTimestamp(pcommon.NewTimestampFromTime(timestamp))
		} else {
			lr.logger.Warn("Failed to evaluate timestamp", zap.Error(err))
		}
	}

	if err := lr.nextConsumer.ConsumeLogs(context.Background(), logs); err != nil {
		lr.logger.Error("Failed to consume logs", zap.Error(err))
		return
	}
}

func (lr *logsReceiver) evalTimestamp(tCtx *ottllog.TransformContext) (time.Time, error) {
	timestampValue, err := lr.timestampExp.Eval(context.Background(), tCtx)
	if err != nil {
		return time.Time{}, err
	}

	v, ok := timestampValue.(time.Time)
	if !ok {
		return time.Time{}, fmt.Errorf("parsed value is not time.Time but %T", timestampValue)
	}

	return v, nil
}

func waitToken(ctx context.Context, token mqtt.Token) error {
	ctx, cancel := context.WithTimeout(ctx, tokenTimeout)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			if token.WaitTimeout(10 * time.Millisecond) {
				return token.Error()
			}
		}
	}
}
