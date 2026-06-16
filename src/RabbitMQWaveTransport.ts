import { randomUUID } from 'crypto';
import { RabbitMQBase, type RabbitMQConfig } from './RabbitMQBase';
import { buildCloudEvent } from './CloudEvent';
import type { WaveLogger } from './Logging';
import { NoopWaveLogger } from './Logging';
import type { WavePropagator } from './Propagation';
import { NoopWavePropagator } from './Propagation';
import type { WaveTracer } from './Tracing';
import { NoopWaveTracer } from './Tracing';
import {
  type AsyncMessage,
  type Consumer,
  type ConsumerHandler,
  ConsumerStatus,
  type Envelope,
  type MessageBody,
  type Publisher,
  type PublisherProps,
  type RPCClient,
  type RPCProps,
} from 'rabbitmq-client';
import type {
  BaseMessageHandler,
  ExecutionContext,
  ListenerOptions,
  Unsubscribe,
  WaveCommandMessage,
  WaveEventMessage,
  WaveMessageKind,
  WaveQueryMessage,
  WaveTransport,
  WaveErrorResponse,
  RPCResponse,
} from './WaveTransport';

/**
 * A RabbitMQ-backed implementation of the Wave transport interface.
 *
 * This class provides a complete message bus implementation using RabbitMQ as the
 * underlying broker. It supports three message patterns:
 * - **Events**: Fire-and-forget notifications published to exchanges.
 * - **Commands**: Request/response or fire-and-forget actionable messages.
 * - **Queries**: Synchronous data retrieval operations.
 *
 * Messages are serialized as CloudEvents v1.0 and support distributed tracing
 * through the {@link WaveTracer} and {@link WavePropagator} interfaces.
 *
 * @example
 * ```typescript
 * import createBus from '@moj/bus/rabbitmq';
 *
 * const bus = createBus({ prefetchCount: 10 });
 *
 * // Send an event
 * await bus.sendEvent({
 *   namespace: 'Billing.Invoice',
 *   name: 'InvoiceCreated',
 *   kind: 'event',
 *   payload: { invoiceId: '123' }
 * });
 *
 * // Handle an event
 * await bus.addEventListener('Billing.Invoice', 'InvoiceCreated', async (msg) => {
 *   console.log('New invoice:', msg);
 * });
 *
 * // Send an event with context
 * await bus.sendEvent({
 *   namespace: 'Billing.Invoice',
 *   name: 'InvoiceCreated',
 *   kind: 'event',
 *   payload: { invoiceId: '123' },
 *   context: {
 *     aggregateId: 'invoice-123',
 *     aggregateType: 'Invoice',
 *     userId: 'user-456'
 *   }
 * });
 *
 * // Send a command with response
 * const result = await bus.sendCommand({
 *   namespace: 'Billing.Invoice',
 *   name: 'GetInvoice',
 *   kind: 'command',
 *   payload: { id: '123' },
 *   awaitResponse: true
 * });
 * ```
 */
export class RabbitMQWaveTransport
  extends RabbitMQBase
  implements WaveTransport
{
  /**
   * A Map of event publishers, keyed by namespace/exchange name.
   *
   * @description
   * This attribute stores Publisher instances responsible for publishing CloudEvents
   * (fire-and-forget notifications) to RabbitMQ exchanges. Each publisher is configured
   * for a specific namespace, enabling event-driven communication between services.
   *
   * @usecase
   * Used when sending asynchronous events that don't require a response. For example,
   * when a user completes an order, an "OrderCompleted" event is published to notify
   * other services (inventory, shipping, analytics) without waiting for acknowledgment.
   */
  private readonly eventPublisher: Map<string, Publisher> = new Map();

  /**
   * A Map of command publishers, keyed by namespace/exchange name.
   *
   * @description
   * This attribute stores Publisher instances for sending commands (request-response
   * patterns) to RabbitMQ. Unlike events, commands expect a response from the recipient.
   * Each publisher is configured with confirm mode to ensure message delivery.
   *
   * @usecase
   * Used when invoking actions that require confirmation. For example, sending a
   * "CalculateTax" command to a tax service expects a "TaxCalculationResult" response.
   * The publisher ensures the command was successfully delivered before sending more.
   */
  private readonly commandPublisher: Map<string, Publisher> = new Map();

  /**
   * A Map of RPC clients dedicated to handling command responses, keyed by namespace.
   *
   * @description
   * Each RPC client is configured with a unique reply queue for receiving command
   * responses. It includes a message handler that processes incoming responses and
   * resolves the corresponding pending command promise.
   *
   * @usecase
   * When a "GetUserProfile" command is sent, the commandClient listens on the
   * pre-established reply queue for the response. Once the response arrives, it
   * matches it using the correlationId and resolves the waiting promise.
   */
  private readonly commandClient: Map<string, RPCClient> = new Map();

  /**
   * A Map of RPC clients dedicated to handling query responses, keyed by namespace.
   *
   * @description
   * Similar to commandClient, but for query operations. Queries are typically
   * idempotent read operations that expect immediate data responses. The client
   * is configured with a reply queue and handler for query-specific response processing.
   *
   * @usecase
   * Used for synchronous data retrieval operations. For example, a "GetProductById"
   * query expects the product details as a response. The queryClient manages the
   * reply queue and matches responses using correlationIds.
   */
  private readonly queryClient: Map<string, RPCClient> = new Map();

  /**
   * The WaveTracer instance used for distributed tracing.
   *
   * @description
   * This tracer creates and manages trace contexts for distributed tracing across
   * service boundaries. It generates trace IDs and span IDs to correlate requests
   * across multiple services in the system.
   *
   * @usecase
   * When processing a command or query, the tracer creates a new span to track
   * the operation. This span includes context that can be propagated to downstream
   * services, enabling end-to-end observability and performance monitoring.
   */
  private readonly tracer: WaveTracer;

  /**
   * The WavePropagator instance used for propagating distributed tracing context.
   *
   * @description
   * This propagator injects trace context into outgoing messages (headers/extensions)
   * and extracts context from incoming messages. It ensures tracing information
   * travels with requests across service boundaries.
   *
   * @usecase
   * When sending a command, the propagator injects the current trace context into
   * the CloudEvent extensions. When a service receives this command, it extracts
   * the context and creates a child span, maintaining the trace chain.
   */
  private readonly propagator: WavePropagator;

  /**
   * A Map of event listeners, keyed by routing key/subscription ID.
   *
   * @description
   * Stores registered event listener configurations, including the RabbitMQ consumer
   * function and listener options. Each listener subscribes to events matching a
   * specific routing key pattern within a namespace.
   *
   * @usecase
   * When a service wants to react to "OrderCreated" events, it registers an event
   * listener. The Map tracks this subscription, enabling cleanup during shutdown
   * and preventing duplicate registrations for the same routing key.
   */
  private readonly eventListeners = new Map<
    string,
    { handler: Consumer; options: ListenerOptions }
  >();

  /**
   * A Map of command listeners, keyed by routing key/subscription ID.
   *
   * @description
   * Stores registered command listener configurations. These listeners handle incoming
   * commands and are responsible for processing them and sending appropriate responses
   * back through the reply queue. Includes handler functions and listener options.
   *
   * @usecase
   * When a service wants to handle "ProcessPayment" commands, it registers a
   * command listener. The Map tracks the subscription, enabling the service to
   * process commands and send responses while maintaining proper cleanup.
   */
  private readonly commandListeners = new Map<
    string,
    { handler: Consumer; options: ListenerOptions }
  >();

  /**
   * A Map of query listeners, keyed by routing key/subscription ID.
   *
   * @description
   * Stores registered query listener configurations. These listeners handle incoming
   * queries and return data responses. Similar to command listeners but optimized
   * for read operations that don't modify state.
   *
   * @usecase
   * When a service wants to handle "GetInventoryCount" queries, it registers a
   * query listener. The Map tracks this subscription, allowing the service to
   * respond to queries efficiently while maintaining proper lifecycle management.
   */
  private readonly queryListeners = new Map<
    string,
    { handler: Consumer; options: ListenerOptions }
  >();

  /**
   * A Map of reply consumers, keyed by reply queue name.
   *
   * @description
   * Stores consumer functions for reply queues that receive command and query responses.
   * These consumers process incoming responses and match them to pending requests
   * using correlationIds.
   *
   * @usecase
   * When waiting for a command response, a reply consumer on the reply queue
   * intercepts the message, extracts the correlationId, and resolves the pending
   * promise associated with that correlationId. This enables the async response
   * handling mechanism.
   */
  private readonly replyConsumers = new Map<string, Consumer>();

  /**
   * A Map of command reply queue names, keyed by namespace.
   *
   * @description
   * Stores the unique reply queue names created for each namespace to receive command
   * responses. Each namespace gets its own reply queue to ensure command responses
   * are properly routed and isolated.
   *
   * @usecase
   * When a service in the "billing" namespace sends a command, it uses the
   * pre-created "billing.command.reply" queue to receive responses. This ensures
   * responses from different namespaces don't mix and maintains proper message routing.
   */
  private readonly commandReplyQueues = new Map<string, string>();

  /**
   * A Map of query reply queue names, keyed by namespace.
   *
   * @description
   * Stores the unique reply queue names created for each namespace to receive query
   * responses. Separate from command reply queues to maintain clear separation
   * between command and query response handling.
   *
   * @usecase
   * When a service in the "inventory" namespace sends a query, it uses the
   * "inventory.query.reply" queue to receive responses. This separation helps
   * with monitoring, debugging, and ensures query responses don't interfere with
   * command responses.
   */
  private readonly queryReplyQueues = new Map<string, string>();

  /**
   * Creates a new RabbitMQWaveTransport instance.
   *
   * @param config - RabbitMQ connection configuration.
   * @param tracer - Distributed tracing implementation. Defaults to no-op.
   * @param logger - Logging implementation. Defaults to no-op.
   * @param propagator - Trace context propagator. Defaults to no-op.
   */
  constructor(
    config: RabbitMQConfig,
    tracer: WaveTracer = new NoopWaveTracer(),
    logger: WaveLogger = new NoopWaveLogger(),
    propagator: WavePropagator = new NoopWavePropagator()
  ) {
    super(config, logger);
    this.tracer = tracer;
    this.propagator = propagator;
  }

  /**
   * Called when the RabbitMQ connection is (re)established.
   *
   * Clears all publisher, client, and queue maps so they can be recreated
   * fresh for the new connection.
   */
  protected override onConnectionEstablished(): void {
    super.onConnectionEstablished();
    this.eventPublisher.clear();
    this.commandPublisher.clear();
    this.commandClient.clear();
    this.queryClient.clear();
    this.queryListeners.clear();
    this.replyConsumers.clear();
    this.commandReplyQueues.clear();
    this.queryReplyQueues.clear();
  }

  /**
   * Called when a connection or channel error occurs.
   *
   * Logs the error via the injected logger and clears all publisher/client maps
   * so they can be recreated on reconnection.
   *
   * @param error - The error that occurred.
   */
  protected override onConnectionError(error: Error): void {
    super.onConnectionError(error);
    // Clear publishers and clients - they will be recreated on reconnection
    this.eventPublisher.clear();
    this.commandPublisher.clear();
    this.commandClient.clear();
    this.queryClient.clear();
    this.queryListeners.clear();
    this.replyConsumers.clear();
    this.commandReplyQueues.clear();
    this.queryReplyQueues.clear();
  }

  /**
   * Ensures an event publisher exists for the given exchange, creating one if necessary.
   * @internal
   */
  private async setupEventPublisher(exchange: string): Promise<Publisher> {
    return this.registerPublisher(this.eventPublisher, exchange, 'event');
  }

  /**
   * Ensures a command publisher exists for the given exchange, creating one if necessary.
   * @internal
   */
  private async setupCommandPublisher(exchange: string): Promise<Publisher> {
    return this.registerPublisher(this.commandPublisher, exchange, 'command');
  }

  /**
   * Ensures an RPC client exists for the given exchange, creating one if necessary.
   * @internal
   */
  private async setupCommandClient(exchange: string): Promise<RPCClient> {
    return this.registerClient(this.commandClient, exchange, 'command');
  }

  /**
   * Ensures an RPC client exists for query responses, creating one if necessary.
   * @internal
   */
  private async setupQueryClient(exchange: string): Promise<RPCClient> {
    return this.registerClient(this.queryClient, exchange, 'query');
  }

  /**
   * Registers (or re-registers) an RPC client in the given map.
   *
   * If a client already exists for the exchange, it is closed and removed before
   * creating a new one with confirm mode enabled.
   *
   * @param listenerMap - The map to store the client in.
   * @param exchange - The exchange name.
   * @param kind - The message kind (command or query).
   * @param options - Optional RPC client overrides.
   * @returns The (new or existing) RPC client.
   * @internal
   */
  private async registerClient(
    listenerMap: Map<string, RPCClient>,
    exchange: string,
    kind: WaveMessageKind,
    options?: RPCProps
  ): Promise<RPCClient> {
    if (listenerMap.has(exchange)) {
      await listenerMap
        .get(exchange)
        ?.close()
        .catch(() => undefined);
      listenerMap.delete(exchange);
    }

    const defaultOptions: RPCProps = {
      confirm: true,
      maxAttempts: 3,
    };
    const rpcClient = this.createRPCClient({ ...defaultOptions, ...options });
    listenerMap.set(exchange, rpcClient);

    this.logger.debug('rabbitmq.rpcClient.ready', {
      exchange,
      kind,
    });
    return rpcClient;
  }

  /**
   * Publishes a CloudEvent to the appropriate RabbitMQ exchange.
   *
   * The message is wrapped in a CloudEvent v1.0 envelope and published as
   * fire-and-forget (no response expected). Trace context from the propagator
   * is injected into the CloudEvent extensions.
   *
   * @param message - The event message containing namespace, name, and payload.
   * @returns A promise that resolves when the event has been published.
   *
   * @example
   * ```typescript
   * await bus.sendEvent({
   *   namespace: 'Billing.Invoice',
   *   name: 'InvoiceCreated',
   *   kind: 'event',
   *   payload: { invoiceId: '123', amount: 100 },
   *   context: {
   *     aggregateId: 'invoice-123',
   *     userId: 'user-456'
   *   }
   * });
   * ```
   */
  public async sendEvent<TPayload = unknown>(
    message: WaveEventMessage<TPayload>
  ): Promise<void> {
    return this.tracer.startActiveSpan('wave.sendEvent', async () => {
      await this.ensureConnected();

      const exchange: string = this.getExchange(message.namespace);

      // Ensure message has an ID
      const messageWithId = {
        ...message,
        id: message.id ?? randomUUID(),
      };

      if (!this.eventPublisher.has(exchange)) {
        await this.setupEventPublisher(exchange);
      }

      const cloudEvent = buildCloudEvent('event', messageWithId, {
        source: `wave://${message.namespace}`,
        extensions: this.propagator.inject(),
      });

      // TODO: check other Envelope type (from rabbitmq-client) options
      await this.eventPublisher.get(exchange)?.send(
        {
          exchange,
          mandatory: true,
          routingKey: this.getRoutingKey(message.namespace, message.name),
        },
        cloudEvent
      );
    });
  }

  /**
   * Checks if an RPC response contains an error and throws it.
   *
   * @param response - The response body from an RPC call
   * @throws RemoteServiceError if the response contains an error
   */
  private checkAndThrowRemoteError(response: any): void {
    if (response?.error) {
      const errorData: WaveErrorResponse = response.error;
      const error = new Error(errorData.message);
      error.name = 'RemoteServiceError';
      (error as any).type = errorData.type;
      (error as any).code = errorData.code;
      (error as any).context = errorData.context;
      (error as any).remoteStack = errorData.stack;
      throw error;
    }
  }

  /**
   * Sends a command through the underlying broker.
   *
   * If `message.awaitResponse` is true, a request/response pattern is used:
   * the command is sent via an RPC client, and this method waits for the
   * handler's response on a reply queue. Otherwise the command is fire-and-forget
   * using the command publisher.
   *
   * The message is wrapped in a CloudEvent v1.0 envelope. Trace context and
   * correlation metadata are injected into the CloudEvent extensions.
   *
   * @param message - The command message to send.
   * @returns A promise that resolves with the response (if awaiting) or `undefined`.
   *
   * @example
   * ```typescript
   * // RPC-style with response
   * const result = await bus.sendCommand({
   *   namespace: 'Billing.Invoice',
   *   name: 'GetInvoice',
   *   kind: 'command',
   *   payload: { id: '123' },
   *   awaitResponse: true,
   *   context: {
   *     correlationId: 'corr-123',
   *     userId: 'user-456'
   *   }
   * });
   *
   * // Fire-and-forget
   * await bus.sendCommand({
   *   namespace: 'Billing.Invoice',
   *   name: 'CreateInvoice',
   *   kind: 'command',
   *   payload: { amount: 100 }
   * });
   * ```
   */
  public async sendCommand<TPayload = unknown, TResponse = unknown>(
    message: WaveCommandMessage<TPayload, TResponse>
  ): Promise<TResponse | void> {
    return this.tracer.startActiveSpan('wave.sendCommand', async () => {
      await this.ensureConnected();

      // The exchange for destination namespace
      const exchange = this.getExchange(message.namespace);

      // If we expect a response, use commandClient (RPC pattern)
      if (message.awaitResponse === true) {
        if (!this.commandClient.has(exchange)) {
          await this.setupCommandClient(exchange);
        }

        const replyQueue = this.getReplyQueueName(exchange, 'command');
        const correlationId = message.context?.correlationId ?? randomUUID();

        // Build cloud event with reply metadata - use dedicated command reply queue
        const namespace = message.namespace.split('.')[0] ?? 'default'; // TODO: what we should do with this situation?

        const messageWithId = {
          ...message,
          id: message.id ?? randomUUID(),
        };

        const cloudEventWithMetadata = buildCloudEvent('command', messageWithId, {
          source: `wave://${message.namespace}`, // FIXME: invalid!! The source namespace is not "message.namespace"!
          extensions: {
            ...this.propagator.inject(),
            correlationId,
            replyQueue,
            ...(message.context?.messageId && { messageId: message.context.messageId }),
            ...(message.context?.awaitedEvents && { awaitedEvents: message.context.awaitedEvents }),
          },
        });

        const sendOptions: Envelope = {
          exchange,
          routingKey: this.getRoutingKey(namespace, message.name),
        };
        // TODO: Take a look at the shutdown scenario. The exchange isn’t ours — it belongs to the destination.
        //       So what are we supposed to do in the shutdown method for the commandClient items?
        const res: AsyncMessage = await this.commandClient
          .get(exchange)!
          .send(sendOptions, cloudEventWithMetadata);

        this.checkAndThrowRemoteError(res?.body);

        return res?.body?.result;
      }

      // Fire-and-forget command: use commandPublisher
      if (!this.commandPublisher.has(exchange)) {
        await this.setupCommandPublisher(exchange);
      }

      const messageWithId = {
        ...message,
        id: message.id ?? randomUUID(),
      };

      const cloudEvent = buildCloudEvent('command', messageWithId, {
        source: `wave://${message.namespace}`,
        extensions: this.propagator.inject(),
      });

      await this.commandPublisher.get(exchange)!.send(
        {
          exchange: exchange,
          routingKey: this.getRoutingKey(message.namespace, message.name),
        },
        cloudEvent
      );

      return undefined as TResponse | void;
    });
  }

  /**
   * Sends a query and awaits its response.
   *
   * Queries are synchronous read operations. The message is sent via an RPC client
   * on a dedicated reply queue, and this method resolves with the handler's result.
   *
   * The message is wrapped in a CloudEvent v1.0 envelope. Trace context and
   * correlation metadata are injected into the CloudEvent extensions.
   *
   * @param message - The query message to send.
   * @returns A promise that resolves with the query result.
   *
   * @example
   * ```typescript
   * const invoice = await bus.sendQuery({
   *   namespace: 'Billing.Invoice',
   *   name: 'GetInvoice',
   *   kind: 'query',
   *   payload: { id: '123' },
   *   context: {
   *     userId: 'user-456',
   *     cacheKey: 'invoice:123'
   *   }
   * });
   * ```
   */
  public async sendQuery<TPayload = unknown, TResponse = unknown>(
    message: WaveQueryMessage<TPayload, TResponse>
  ): Promise<TResponse> {
    return this.tracer.startActiveSpan('wave.sendQuery', async () => {
      await this.ensureConnected();

      const exchange = this.getExchange(message.namespace);
      if (!this.queryClient.has(exchange)) {
        await this.setupQueryClient(exchange);
      }

      const replyQueue = this.getReplyQueueName(exchange, 'query');
      const correlationId = message.context?.correlationId ?? randomUUID();
      const namespace = message.namespace.split('.')[0] ?? 'default'; // FIXME: What is the default namespace at all?!

      const messageWithId = {
        ...message,
        id: message.id ?? randomUUID(),
      };

      const cloudEvent = buildCloudEvent('query', messageWithId, {
        source: `wave://${message.namespace}`,
        extensions: {
          ...this.propagator.inject(),
          correlationId,
          replyQueue,
        },
      });

      const sendOptions: Envelope = {
        exchange,
        routingKey: this.getRoutingKey(namespace, message.name),
      };

      const res = await this.queryClient
        .get(exchange)!
        .send(sendOptions, cloudEvent);

      // Check if response contains an error and throw it
      this.checkAndThrowRemoteError(res?.body);

      return res?.body?.result;
    });
  }

  /**
   * Send an RPC response to a reply queue.
   * Used internally by publishEvent and reject when RPC context is present.
   *
   * @param replyQueue - Name of the reply queue
   * @param response - RPC response object
   * @returns A promise that resolves when the response is sent
   */
  public async sendToReplyQueue(
    replyQueue: string,
    response: RPCResponse
  ): Promise<void> {
    await this.ensureConnected();

    // Get or create a publisher for direct queue sends
    if (!this.eventPublisher.has(replyQueue)) {
      const publisher = this.createPublisher({
        confirm: true,
        maxAttempts: 3,
      });
      this.eventPublisher.set(replyQueue, publisher);
    }

    // Send directly to the reply queue (no exchange, no routing key)
    await this.eventPublisher.get(replyQueue)!.send(
      {
        queue: replyQueue,
      },
      response
    );

    this.logger.debug('rabbitmq.rpc.response.sent', {
      replyQueue,
      responseType: response.type,
    });
  }

  /**
   * Registers a handler for commands of a specific namespace and name.
   *
   * Creates a RabbitMQ consumer on a dedicated queue. Returns an unsubscribe
   * function that can be called to remove the listener and stop message processing.
   *
   * @param namespace - Bounded context or service name, e.g. `"Billing"`.
   * @param name - Command name, e.g. `"CreateInvoice"`.
   * @param handler - Handler function that processes the command payload.
   * @param options - Optional configuration (autoAck, etc.). Defaults to `{}`.
   * @returns A promise that resolves with an unsubscribe function.
   *
   * @example
   * ```typescript
   * const unsubscribe = await bus.addCommandListener(
   *   'Billing.Invoice',
   *   'CreateInvoice',
   *   async (msg, ctx) => {
   *     console.log('Creating invoice:', msg);
   *   }
   * );
   * // Later: await unsubscribe();
   * ```
   */
  public async addCommandListener(
    namespace: string,
    name: string,
    handler: BaseMessageHandler,
    options: ListenerOptions = {}
  ): Promise<Unsubscribe> {
    // register consumer and return unsubscribe method
    return await this.registerConsumer(
      this.commandListeners,
      this.getExchange(namespace),
      this.getRoutingKey(namespace, name),
      this.getQueueName('command', namespace, name),
      options,
      this.getConsumerHandler(namespace, name, 'command', handler)
    );
  }

  /**
   * Registers a handler for events of a specific namespace and name.
   *
   * Creates a RabbitMQ consumer on a dedicated queue. Returns an unsubscribe
   * function that can be called to remove the listener and stop message processing.
   *
   * @param namespace - Bounded context or service name, e.g. `"Billing"`.
   * @param name - Event name, e.g. `"InvoiceCreated"`.
   * @param handler - Handler function that processes the event payload.
   * @param options - Optional configuration (autoAck, etc.). Defaults to `{}`.
   * @returns A promise that resolves with an unsubscribe function.
   *
   * @example
   * ```typescript
   * await bus.addEventListener(
   *   'Billing.Invoice',
   *   'InvoiceCreated',
   *   async (msg, ctx) => {
   *     console.log('New invoice:', msg);
   *   }
   * );
   * ```
   */
  public async addEventListener(
    namespace: string,
    name: string,
    handler: BaseMessageHandler,
    options: ListenerOptions = {}
  ): Promise<Unsubscribe> {
    // register consumer and return unsubscriber method
    return await this.registerConsumer(
      this.eventListeners,
      this.getExchange(namespace),
      this.getRoutingKey(namespace, name),
      this.getQueueName('event', namespace, name),
      options,
      this.getConsumerHandler(namespace, name, 'event', handler, options)
    );
  }

  /**
   * Registers a handler for queries of a specific namespace and name.
   *
   * Creates a RabbitMQ consumer on a dedicated queue. Returns an unsubscribe
   * function that can be called to remove the listener and stop message processing.
   *
   * @param namespace - Bounded context or service name, e.g. `"Billing"`.
   * @param name - Query name, e.g. `"GetInvoice"`.
   * @param handler - Handler function that processes the query and returns a result.
   * @param options - Optional configuration (autoAck, etc.). Defaults to `{}`.
   * @returns A promise that resolves with an unsubscribe function.
   *
   * @example
   * ```typescript
   * await bus.addQueryListener(
   *   'Billing.Invoice',
   *   'GetInvoice',
   *   async (msg, ctx) => {
   *     return { id: msg.id, status: 'paid' };
   *   }
   * );
   * ```
   */
  public async addQueryListener(
    namespace: string,
    name: string,
    handler: BaseMessageHandler,
    options: ListenerOptions = {}
  ): Promise<Unsubscribe> {
    // register consumer and return unsubscriber method
    return await this.registerConsumer(
      this.queryListeners,
      this.getExchange(namespace),
      this.getRoutingKey(namespace, name),
      this.getQueueName('query', namespace, name),
      options,
      this.getConsumerHandler(namespace, name, 'query', handler, options)
    );
  }

  /**
   * Registers a publisher for the given exchange in the given map.
   *
   * Creates a new publisher with confirm mode enabled, retry on failure up to
   * 3 attempts, and a topic exchange. Binds the publisher's return and retry
   * events to the logger. If a publisher already exists for the exchange,
   * it is closed and removed first.
   *
   * @param listenerMap - The map to store the publisher in.
   * @param exchange - The exchange name (and key).
   * @param kind - The message kind (event or command).
   * @param options - Optional publisher overrides.
   * @returns The (new or existing) publisher.
   * @internal
   */
  private async registerPublisher(
    listenerMap: Map<string, Publisher>,
    exchange: string,
    kind: WaveMessageKind,
    options?: PublisherProps
  ): Promise<Publisher> {
    // If already registered, remove the old one
    if (listenerMap.has(exchange)) {
      const existing = listenerMap.get(exchange);
      if (existing) {
        await existing.close().catch(() => undefined);
        listenerMap.delete(exchange);
      }
    }

    const defaultOptions: PublisherProps = {
      confirm: true,
      maxAttempts: 3,
      exchanges: [{ exchange, type: 'topic' }],
    };
    const publisher: Publisher = this.createPublisher({
      ...defaultOptions,
      ...options,
    });

    publisher.on('basic.return', (message) => {
      this.logger.warn('rabbitmq.publisher.return', {
        exchange,
        kind,
        message,
      });
    });
    publisher.on('retry', (error, message) => {
      this.logger.info('rabbitmq.publisher.retry', {
        exchange,
        kind,
        error,
        message,
      });
    });
    listenerMap.set(exchange, publisher);
    this.logger.debug('rabbitmq.publisher.ready', { exchange, kind });
    return publisher;
  }

  /**
   * Internal helper to register a consumer on a specific queue.
   *
   * Creates a consumer on the given queue, binds it to the exchange with the
   * specified routing key, and returns a promise that resolves with an unsubscribe
   * function once the consumer is ready. If a consumer already exists for the
   * queue name, it is closed and removed first.
   *
   * @param listenerMap - The map to store the consumer in.
   * @param exchange - The exchange to bind to.
   * @param routingKey - The routing key for the binding.
   * @param queueName - The queue to create/bind the consumer on.
   * @param options - Consumer options (autoAck, etc.).
   * @param handler - The consumer handler function.
   * @returns A promise that resolves with an unsubscribe function.
   * @internal
   */
  private async registerConsumer(
    listenerMap: Map<string, { handler: Consumer; options: ListenerOptions; close(): Promise<void> }>,
    exchange: string,
    routingKey: string,
    queueName: string,
    options: ListenerOptions,
    handler: ConsumerHandler
  ): Promise<Unsubscribe> {
    // If already registered, remove the old one
    if (listenerMap.has(queueName)) {
      const existing = listenerMap.get(queueName);
      if (existing) {
        await existing.handler.close();
        listenerMap.delete(queueName);
      }
    }

    // const exchange = this.getExchange(routingKey);
    // TODO: when to use direct exchange?!
    // const type = routingKey.includes('.')
    //   ? ((routingKey.split('.')[0] === 'commands' ? 'direct' : 'topic') as
    //       | 'direct'
    //       | 'topic')
    //   : 'topic';
    const type = 'topic';

    // Set up consumer
    const consumer = this.createConsumer(
      {
        queue: queueName,
        exchanges: [{ exchange, type }],
        queueBindings: [{ exchange, routingKey }],
        noAck: !(options?.autoAck ?? false),
      },
      handler
    );

    consumer.start();

    return new Promise((resolve, reject) => {
      consumer.on('ready', () => {
        this.logger.debug('rabbitmq.consumer.ready', {
          exchange,
          type,
          queueName,
          routingKey,
        });

        const unsubscribe = async () => {
          const existing = listenerMap.get(queueName);
          if (existing) {
            await existing.handler.close();
            listenerMap.delete(queueName);
          }
        };
        listenerMap.set(queueName, { handler: consumer, options, close: unsubscribe });
        resolve(unsubscribe);
      });

      consumer.on('error', reject);
    });
  }

  /**
   * Creates a consumer handler for the given namespace, name, and kind.
   *
   * The returned handler:
   * 1. Wraps execution in try/catch to ensure automatic nack on error.
   * 2. Provides an {@link ExecutionContext} with ack/nack methods.
   * 3. If `autoAck` is false and the handler did not explicitly ack/nack,
   *    it auto-acks on success or nacks on error.
   * 4. If the incoming message has a `correlationId` in extensions and the
   *    handler returned a result, the result is sent back via the reply mechanism.
   *
   * @param namespace - The bounded context.
   * @param name - The message name.
   * @param kind - The message kind.
   * @param handler - The user-provided handler function.
   * @param options - Listener options (autoAck, etc.).
   * @returns A consumer handler function.
   * @internal
   */
  private getConsumerHandler(
    namespace: string,
    name: string,
    kind: WaveMessageKind,
    handler: BaseMessageHandler,
    options: ListenerOptions = {}
  ): ConsumerHandler {
    return async (
      rawMessage: AsyncMessage,
      reply: (body: MessageBody, envelope?: Envelope) => Promise<void>
    ): Promise<void> => {
      let isAckSent = false;
      const context: ExecutionContext = {
        message: rawMessage,
        ack: () => {
          isAckSent = true;
          return ConsumerStatus.ACK;
        },
        nack: (requeue = false) => {
          isAckSent = true;
          return requeue ? ConsumerStatus.REQUEUE : ConsumerStatus.DROP;
        },
      };

      let result;
      let errorResponse;
      try {
        result = await handler(rawMessage.body.data, context);
        if (!options.autoAck && !isAckSent) {
          context.ack();
        }
      } catch (error) {
        // Serialize error into wire format
        errorResponse = {
          type: error instanceof Error ? error.constructor.name : 'Error',
          message: error instanceof Error ? error.message : String(error),
          code: (error as any).code,
          context: (error as any).context,
          stack: process.env.NODE_ENV === 'development' && error instanceof Error ? error.stack : undefined,
        };

        if (!options.autoAck && !isAckSent) {
          context.nack(false);
        }
      }

      // Handle response if requested using reply parameter
      const correlationId = rawMessage.body.extensions?.correlationId;
      if ((result !== undefined || errorResponse !== undefined) && correlationId) {
        try {
          const replyMessage = errorResponse
            ? {
                kind: 'command-response',
                correlationId,
                error: errorResponse,
                timestamp: new Date().toISOString(),
              }
            : {
                kind: 'command-response',
                correlationId,
                result,
                timestamp: new Date().toISOString(),
              };

          console.log({ replyMessage });
          // Use the reply parameter from ConsumerHandler to send response
          await reply(replyMessage);
        } catch (error) {
          this.logger.error('command.reply.error', {
            namespace,
            name,
            kind,
            error: (error as Error).message,
          });
        }
      }

      void result; // Result used for sending replies, not returned
    };
  }

  /**
   * Returns the exchange name for a given namespace.
   *
   * The format is `wave.<first-part-of-namespace>` (e.g., `"wave.billing"` for
   * `"Billing.Invoice"`). Falls back to `"wave.default"` if the namespace is empty.
   *
   * @param namespace - The namespace to derive the exchange from.
   * @returns The exchange name.
   * @internal
   */
  private getExchange(namespace?: string): string {
    const prefix: string = 'wave';
    const suffix: string = namespace?.split('.')[0] ?? 'default';
    return `${prefix}.${suffix}`;
  }

  /**
   * Returns the routing key for a namespace and name.
   *
   * The format is `<namespace>.<name>` (e.g., `"Billing.Invoice.CreateInvoice"`).
   *
   * @param namespace - The bounded context.
   * @param name - The message name.
   * @returns The routing key.
   * @internal
   */
  private getRoutingKey(namespace: string, name: string): string {
    return `${namespace}.${name}`;
  }

  /**
   * Returns the queue name for a given kind, namespace, and name.
   *
   * The format is `wave.<kind>.queue.<namespace>.<name>`
   * (e.g., `"wave.event.queue.Billing.Invoice.InvoiceCreated"`).
   *
   * @param kind - The message kind (event, command, or query).
   * @param namespace - The bounded context.
   * @param name - The message name.
   * @returns The queue name.
   * @internal
   */
  private getQueueName(
    kind: WaveMessageKind,
    namespace: string,
    name: string
  ): string {
    return `wave.${kind}.queue.${namespace}.${name}`;
  }

  /**
   * Returns the reply queue name for an exchange and kind.
   *
   * The format is `<exchange>.<kind>-response`
   * (e.g., `"wave.billing.command-response"`).
   *
   * @param exchange - The exchange name.
   * @param kind - The message kind (command or query).
   * @returns The reply queue name.
   * @internal
   */
  private getReplyQueueName(exchange: string, kind: WaveMessageKind): string {
    return `${exchange}.${kind}-response`;
  }

  /**
   * Gracefully shuts down all publishers, RPC clients, and reply consumers.
   *
   * Called by {@link RabbitMQBase.shutdown} before closing the connection.
   * Awaits closure of all publisher and client instances, then reply consumers.
   * Errors during shutdown are suppressed to ensure all resources are attempted
   * for closure.
   *
   * @internal
   */
  protected override async beforeShutdown(): Promise<void> {
    const allPublishersAndClients = [
      ...this.eventPublisher.values(),
      ...this.commandPublisher.values(),
      ...this.commandClient.values(),
      ...this.queryClient.values(),
      ...this.queryListeners.values(),
    ];

    const cancel = async function (p: any) {
      try {
        await p?.close()
      } catch (error) {
        console.log('can not cancel the listener');
        console.log(error);
      }
    }
    await Promise.all(
      allPublishersAndClients
        .filter(Boolean)
        .map(cancel)
    );

    const replyConsumerPromises = Array.from(this.replyConsumers.values()).map(
      (consumer) => consumer.close().catch(() => undefined)
    );

    await Promise.all(replyConsumerPromises);
  }
}
