# MQTT Receiver

The MQTT receiver subscribes to one or more MQTT topics and converts each incoming
MQTT message into an OpenTelemetry log record.

The MQTT message payload is stored in the log body as a UTF-8 string.

## Configuration

| Name        | Required | Description                                                                                                                                                         |
|-------------|----------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `broker`    | Yes      | MQTT broker URL. Only the `tcp://` scheme is supported, and the URL must include a port.                                                                            |
| `topics`    | Yes      | List of MQTT topics to subscribe to.                                                                                                                                |
| `username`  | No       | Username used for broker authentication.                                                                                                                            |
| `password`  | No       | Password used for broker authentication.                                                                                                                            |
| `timestamp` | No       | [OTTL](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/v0.144.0/pkg/ottl) statement for extracting log record timestamps from the MQTT payload. If omitted, the observed time is used. |

### Example configuration

```yaml
receivers:
  mqtt:
    broker: ${env:MQTT_BROKER:-tcp://127.0.0.1:1883}
    topics: ["${env:MQTT_TOPIC}"]
    username: ${env:MQTT_USERNAME:-}
    password: ${env:MQTT_PASSWORD:-}
    timestamp: Time(ParseJSON(log.body.string)["time"], "%Y-%m-%dT%H:%M:%S%z")
```

## Emitted attributes

The receiver emits attributes at both the resource and log record levels.

### Resource attributes

| Name             | Description         |
|------------------|---------------------|
| `server.address` | Broker hostname     |
| `server.port`    | Broker port         |
| `url.scheme`     | Broker URL scheme   |
| `mqtt.topic`     | Topic name          |
| `mqtt.username`  | Configured username |

### Log record attributes

| Name                     | Description                                  |
|--------------------------|----------------------------------------------|
| `messaging.message.id`   | MQTT message ID                              |
| `mqtt.message.qos`       | Message QoS level                            |
| `mqtt.message.duplicate` | Whether the message is marked as a duplicate |
| `mqtt.message.retained`  | Whether the message is marked as retained    |

## Examples

Several example configurations are available in the [`examples`](./examples) directory.
