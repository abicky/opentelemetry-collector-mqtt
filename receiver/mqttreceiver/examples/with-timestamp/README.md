# Timestamp example

This example extracts the log timestamp from the MQTT message payload.
The message payload must be in JSON format and must contain the `data.timestamp` field in ISO 8601 format, for example:

```json
{
  "data": {
    "timestamp": "2026-01-23T12:34:56Z"
  }
}
```

## Environment variables

| Name          | Required | Default              | Description                              |
|---------------|----------|----------------------|------------------------------------------|
| MQTT_BROKER   | Yes      | tcp://127.0.0.1:1883 | MQTT broker URL.                         |
| MQTT_TOPIC    | Yes      |                      | MQTT topics to subscribe to.             |
| MQTT_USERNAME | No       |                      | Username used for broker authentication. |
| MQTT_PASSWORD | No       |                      | Password used for broker authentication. |
