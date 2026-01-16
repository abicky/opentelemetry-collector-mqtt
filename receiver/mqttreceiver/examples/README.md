# MQTT Receiver Examples

## Usage

In each example directory, you can build a custom OpenTelemetry Collector binary with this receiver
by running:

```sh
go tool builder --config=./builder-config.yaml
```

This command builds an `otelcol` binary in the `build` directory.

Once the binary is built, run it with the example configuration and the appropriate environment variables:

```sh
build/otelcol --config=collector-config.yaml
```

For further information about building a custom collector, see
[Build a custom Collector with OpenTelemetry Collector Builder](https://opentelemetry.io/docs/collector/extend/ocb/).

## Start MQTT broker

You can start a local MQTT broker using Docker Compose in this directory:

```sh
docker compose up -d
```

## Publish message to broker

To send a test message to the broker, run:

```sh
docker compose exec mosquitto \
  mosquitto_pub -t 'test/topic' -m 'Hello World'
```
