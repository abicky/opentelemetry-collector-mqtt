package mqttreceiver

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var mqttBrokerContainer *testcontainers.DockerContainer

func beforeAll() {
	mqttBrokerContainer = startMQTTBroker()
}

func afterAll() {
	stopMQTTBroker(mqttBrokerContainer)
}

func startMQTTBroker() *testcontainers.DockerContainer {
	ctx := context.Background()
	c, err := testcontainers.Run(
		ctx,
		"eclipse-mosquitto:2.0.22",
		testcontainers.WithExposedPorts("1883/tcp"),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("1883/tcp"),
		),
		// Bind 1883 port for TestComponentLifecycle, where the port cannot be set dynamically
		testcontainers.WithHostConfigModifier(func(hostConfig *container.HostConfig) {
			hostConfig.PortBindings = nat.PortMap{"1883/tcp": []nat.PortBinding{
				{
					HostIP:   "127.0.0.1",
					HostPort: "1883",
				},
			}}
		}),
		testcontainers.WithFiles(testcontainers.ContainerFile{
			Reader: strings.NewReader(`listener 1883 0.0.0.0
allow_anonymous true`),
			ContainerFilePath: "/mosquitto/config/mosquitto.conf",
		}),
	)
	if err != nil {
		panic(err)
	}

	return c
}

func stopMQTTBroker(c *testcontainers.DockerContainer) {
	if err := testcontainers.TerminateContainer(c); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to terminate container %q: %v\n", c.ID, err)
	}
}
