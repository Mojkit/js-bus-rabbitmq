# TODO


### title (priority)

description

### pass integration tests (P1)

RabbitMQIntegration.test.ts

#### command tests

#### query tests

### ack/noAck (P2)

What is the exact config for acknowledgment?
In different queues: commands, queries, events, replies

### The reply of command (kind: `command-response`) is not in cloud-event format (P3)

### When to acknowledge the command-response? (P2)

In CreateCommandReplyHandler, when processing the response, should it resolve before response handler,
or after it?

```js
await rawMessage.ack();
pending.resolve(messageData.result);

// or

pending.resolve(messageData.result);
await rawMessage.ack();
```

### Service name for reply queues (P1)

In `getCommandReplyQueue` method, the `namespace` argument should be the source namespace,
so it’s better to use a unique name from the source service name

### check type definitions (P3)

- do not use type as `any`
- Use `rabbitmq-client` package type, instead of some weired types like: `Parameters<Connection['createPublisher']>[0]`.

### The routingKey and queueName can be same for commands (P2)

current status:

```js
rabbitmq.consumer.ready {
  exchange: "wave.Test",
  type: "topic",
  queue: "wave.command.queue.Test.testCommand",
  routingKey: "Test.testCommand",
}
```

### Create an idempotent package for interfaces (P4)

We want to build `moj-bus-nats` package too. So there is need to separate interface classes to another package that
should import into both packages

### Add more integration test (P1)

#### Test shutdown scenario (P1)

The `rabbitmq-client` itself await for channels to close, is there a need to some custom shutdown script
(like `beforeShutdown`)?

#### Test reconnect scenario (P1)

Does `rabbitmq-client` handles the reconnection?
Is there any need to `onConnectionEstableshed` method?

#### Two instances tests (P2)

Run two instances of bus, and test with two different namespaces. Some more realistic tests.

#### Test ack/nack (P1)

Test two type of nack (with and without requeue).

### Customizable prefix (P3)

We’ve been using “wave” as the prefix for all queues and exchanges,
but it really should be set in the initialization options.

### Quorum queues (P1)

Use RabbitMQ v4 features.
Check options of each queue.

### Review CloudEvent structure (P2)

Change source, type, ... if needed.

### Dead Letter Queue (DLQ) strategy (P1)

```js
interface RabbitMQConfig {
  // ... existing ...
  dlq?: {
    enabled: boolean;
    maxRetries: number;
    backoffMs: number;
    backoffMultiplier: number;
  };
}
```
