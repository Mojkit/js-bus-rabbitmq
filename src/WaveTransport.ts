/**
 * @file WaveTransport.ts - Wave-level transport abstraction on top of RabbitMQ.
 *
 * This module defines the core message types, interfaces, and contracts for the Wave bus
 * pattern. It supports three message kinds: commands (request/response), events
 * (fire-and-forget), and queries (synchronous data retrieval).
 *
 * The design intentionally keeps the transport layer independent from CloudEvents
 * and OpenTelemetry concerns; those are layered on top in other modules.
 */

export type WaveMessageKind = 'command' | 'event' | 'query';

/**
 * Base message structure shared by commands, events, and queries.
 *
 * @template TPayload - The type of the business payload carried in the message.
 * @template TContext - Out-of-band metadata such as user, tenant, or correlation IDs.
 */
export interface WaveBaseMessage<
  TPayload = unknown,
  TContext extends Record<string, any> = Record<string, any>,
> {
  /**
   * Message id. Random UUID by default.
   */
  id?: string;
  /**
   * DDD bounded context or service name, e.g. "Billing.Invoice".
   */
  namespace: string;

  /**
   * Logical name of the command/event/query, e.g. "CreateInvoice".
   */
  name: string;

  /**
   * Business payload.
   */
  payload: TPayload;

  /**
   * Out-of-band metadata (user, tenant, correlation IDs, etc.).
   * This will later map into CloudEvents extensions and tracing context.
   */
  context?: TContext;
}

/**
 * Base context structure for all Wave messages.
 * Contains DDD aggregate tracking and standard cross-cutting concerns.
 */
export interface WaveBaseContext {
  /** ID of the aggregate this message relates to (e.g., "user-123", "order-456") */
  aggregateId?: string;

  /** List of event names the sender is interested in (for sender's internal use) */
  events?: string[];

  /** Correlation ID for request tracing across services */
  correlationId?: string;

  /** ID of the message that caused this one (causality tracking) */
  causationId?: string;

  /** User who initiated the action */
  userId?: string;

  /** Allow custom extension fields */
  [key: string]: any;
}

/**
 * A command message – an actionable request that may optionally expect a response.
 *
 * Commands represent intent to perform an action. If `awaitResponse` is true,
 * the sender expects an RPC-style response; otherwise the command is fire-and-forget.
 *
 * @template TPayload - The type of the command payload.
 * @template TResponse - The expected response type (typing only; not serialized).
 * @template TContext - Out-of-band metadata.
 */
export interface WaveCommandMessage<
  TPayload = unknown,
  // @ts-expect-error - TResponse is used for method signatures, not in message structure
  TResponse = unknown,
  TContext extends WaveBaseContext = WaveBaseContext,
> extends WaveBaseMessage<TPayload, TContext> {
  kind: 'command';

  /**
   * Whether the caller expects a response (RPC-style).
   * When false, the command is fire-and-forget.
   */
  awaitResponse?: boolean;
}

/**
 * Command-specific context extensions.
 */
export interface WaveCommandContext extends WaveBaseContext {
  /** Internal: reply queue for RPC responses */
  replyQueue?: string;

  /** Command execution timeout in milliseconds */
  timeoutMs?: number;

  /** Message ID for RPC correlation (event-driven RPC) */
  messageId?: string;

  /** List of event names that can resolve the RPC promise (event-driven RPC) */
  awaitedEvents?: string[];

  // TResponse exists for typing only; it is not serialized.
}

/**
 * An event message – a fire-and-forget notification about something that happened.
 *
 * Events are used for asynchronous, one-way communication between bounded contexts.
 *
 * @template TPayload - The type of the event payload.
 * @template TContext - Out-of-band metadata.
 */
export interface WaveEventMessage<
  TPayload = unknown,
  TContext extends WaveBaseContext = WaveBaseContext,
> extends WaveBaseMessage<TPayload, TContext> {
  kind: 'event';
}

/**
 * Event-specific context extensions.
 */
export interface WaveEventContext extends WaveBaseContext {
  /** Event schema version for evolution tracking */
  eventVersion?: string;

  /** When the domain event occurred (ISO 8601 timestamp) */
  occurredAt?: string;
}

/**
 * A query message – a synchronous request for data that expects an immediate response.
 *
 * Queries are idempotent read operations, distinct from commands which modify state.
 *
 * @template TPayload - The type of the query payload (e.g. filter criteria).
 * @template TResponse - The expected response type (typing only; not serialized).
 * @template TContext - Out-of-band metadata.
 */
export interface WaveQueryMessage<
  TPayload = unknown,
  // @ts-expect-error - TResponse is used for method signatures, not in message structure
  TResponse = unknown,
  TContext extends WaveBaseContext = WaveBaseContext,
> extends WaveBaseMessage<TPayload, TContext> {
  kind: 'query';

  // TResponse exists for typing only; it is not serialized.
}

/**
 * Query-specific context extensions.
 */
export interface WaveQueryContext extends WaveBaseContext {
  /** Chained methods to execute after the main query handler */
  methods?: Array<{ method: string; args?: any[] }>;

 /** Optional cache key for query results */
 cacheKey?: string;

  /** Cache TTL in seconds */
  cacheTtl?: number;
}

/**
 * Standardized error structure for RPC responses.
 *
 * This structure is used to serialize exceptions across service boundaries,
 * enabling Service B to programmatically handle errors from Service A.
 *
 * @example
 * ```typescript
 * {
 *   type: 'ValidationError',
 *   message: 'Invalid email format',
 *   code: 'INVALID_EMAIL',
 *   context: { field: 'email', value: 'invalid' }
 * }
 * ```
 */
export interface WaveErrorResponse {
  type: string;
  message: string;
  code?: string;
  context?: Record<string, any>;
  stack?: string;
}

/**
 * RPC response structure sent to reply queues.
 * Used for event-driven RPC where handlers can publish events or errors
 * that resolve/reject the RPC promise.
 */
export interface RPCResponse {
  /** Type of response */
  type: 'event' | 'error' | 'result';

  /** Message ID from the original RPC request */
  messageId: string;

  /** Event name (for type='event') */
  eventName?: string;

  /** Event or result payload (for type='event' or type='result') */
  payload?: unknown;

  /** Error details (for type='error') */
  error?: { errorCode: string; message: string; data?: unknown };
}

/**
 * Options for controlling send behaviour.
 */
export interface WaveSendOptions {
  /**
   * Optional timeout (in milliseconds) for RPC-style calls
   * (commands with awaitResponse=true and queries).
   */
  timeoutMs?: number;
}

/**
 * Options for listener registration.
 */
export interface ListenerOptions {
  /**
   * Auto-acknowledge messages after successful handler execution.
   * If true, the consumer send "ack" immediately after message delivering.
   * If false, the handler can call `context.ack()` or `context.nack()` explicitly.
   * In this status, if the handler did not call neither ack neither nack, it will call
   * automatically, by exception nack.
   * Default: false
   */
  autoAck?: boolean;

  // TODO: add other ConsumerProps options rabbitmq-client package
}

/**
 * Execution context provided to message handlers.
 * Allows explicit acknowledgment control.
 */
export interface ExecutionContext {
  /**
   * Acknowledge the message as successfully processed.
   */
  ack(): number;

  /**
   * Negative acknowledge the message, optionally requeueing it.
   * @param requeue - If true, requeue the message for retry. Default: false.
   */
  nack(requeue?: boolean): number;

  /**
   * The raw message metadata (routing key, headers, etc.).
   */
  message: Record<string, any>;
}

/**
 * Handler function type for all message kinds.
 * Receives the typed message and execution context.
 */
/**
 * Handler function type for all message kinds.
 *
 * Receives the typed message and execution context. The handler should return
 * a response when expected (for commands/queries) or void (for events).
 *
 * @template TMessage - The specific Wave message type this handler processes.
 * @template TResponse - The return type of the handler.
 */
export type WaveHandler<TMessage extends WaveBaseMessage, TResponse = void> = (
  message: TMessage,
  context: ExecutionContext
) => Promise<TResponse>;

/**
 * Unregister function returned when a listener is added.
 *
 * Calling it removes the listener and stops message processing for that subscription.
 */
export type Unsubscribe = () => Promise<void>;

/**
 * A base handler function that receives raw message data and execution context.
 * Used internally by listener registration methods.
 */
export type BaseMessageHandler = (
  data: object,
  context: ExecutionContext
) => Promise<object | void>;

/**
 * The core Wave transport contract.
 *
 * Implementations provide sending (commands, events, queries) and listening
 * (command, event, query handlers) capabilities backed by RabbitMQ or another broker.
 *
 * @example
 * ```typescript
 * const bus = createBus();
 *
 * // Send an event
 * await bus.sendEvent({
 *   namespace: 'Billing.Invoice',
 *   name: 'InvoiceCreated',
 *   kind: 'event',
 *   payload: { invoiceId: '123' }
 * });
 *
 * // Register a handler
 * await bus.addEventListener('Billing.Invoice', 'InvoiceCreated', async (msg) => {
 *   console.log(msg);
 * });
 * ```
 */
export interface WaveTransport {
  /**
   * Send a command through the underlying broker.
   *
   * If `message.awaitResponse` is true, this resolves with the handler's response
   * after the command is processed. Otherwise, the command is fire-and-forget and
   * this resolves with `void`.
   *
   * @param message - The command message to send.
   * @param options - Optional send options (e.g., timeout for RPC-style calls).
   * @returns A promise that resolves with the response (if awaiting) or void.
   *
   * @example
   * ```typescript
   * // Fire-and-forget
   * await bus.sendCommand({
   *   namespace: 'Billing.Invoice',
   *   name: 'CreateInvoice',
   *   kind: 'command',
   *   payload: { amount: 100 }
   * });
   *
   * // RPC-style with response
   * const result = await bus.sendCommand({
   *   namespace: 'Billing.Invoice',
   *   name: 'GetInvoice',
   *   kind: 'command',
   *   payload: { id: '123' },
   *   awaitResponse: true
   * });
   * ```
   */
  sendCommand<TPayload = unknown, TResponse = unknown>(
    message: WaveCommandMessage<TPayload, TResponse>,
    options?: WaveSendOptions
  ): Promise<TResponse | void>;

  /**
   * Send an RPC response to a reply queue.
   * Used internally by publishEvent and reject when RPC context is present.
   *
   * @param replyQueue - Name of the reply queue
   * @param response - RPC response object
   * @returns A promise that resolves when the response is sent
   */
  sendToReplyQueue(
    replyQueue: string,
    response: RPCResponse
  ): Promise<void>;

  /**
   * Publish an event (fire-and-forget).
   *
   * Events are one-way notifications. This method always resolves with `void`
   * and does not await any response from consumers.
   *
   * @param message - The event message to publish.
   * @returns A promise that resolves when the event has been published.
   *
   * @example
   * ```typescript
   * await bus.sendEvent({
   *   namespace: 'Billing.Invoice',
   *   name: 'InvoiceCreated',
   *   kind: 'event',
   *   payload: { invoiceId: '123', amount: 100 }
   * });
   * ```
   */
  sendEvent<TPayload = unknown>(
    message: WaveEventMessage<TPayload>
  ): Promise<void>;

  /**
   * Send a query and await its response.
   *
   * Queries are synchronous read operations. This method blocks until the
   * handler responds or the configured timeout expires.
   *
   * @param message - The query message to send.
   * @param options - Optional send options (e.g., timeout in milliseconds).
   * @returns A promise that resolves with the query result.
   *
   * @example
   * ```typescript
   * const invoice = await bus.sendQuery({
   *   namespace: 'Billing.Invoice',
   *   name: 'GetInvoice',
   *   kind: 'query',
   *   payload: { id: '123' }
   * });
   * ```
   */
  sendQuery<TPayload = unknown, TResponse = unknown>(
    message: WaveQueryMessage<TPayload, TResponse>,
    options?: WaveSendOptions
  ): Promise<TResponse>;

  /**
   * Register a handler for a command of a specific namespace.
   *
   * Returns an unsubscribe function that can be called to remove the listener
   * and stop message processing. Calling it multiple times has no additional effect
   * after the first call.
   *
   * @param namespace - Bounded context or service name, e.g. `"Billing"`.
   * @param name - Command name, e.g. `"CreateInvoice"`.
   * @param handler - Handler function that processes the command payload.
   * @param options - Optional configuration (autoAck, etc.).
   * @returns A promise that resolves with an unsubscribe function.
   *
   * @example
   * ```typescript
   * const unsubscribe = await bus.addCommandListener(
   *   'Billing.Invoice',
   *   'CreateInvoice',
   *   async (message, context) => {
   *     console.log('Creating invoice:', message);
   *   }
   * );
   *
   * // Later: remove the handler
   * await unsubscribe();
   * ```
   */
  addCommandListener(
    namespace: string,
    name: string,
    handler: BaseMessageHandler,
    options?: ListenerOptions
  ): Promise<Unsubscribe>;

  /**
   * Register a handler for an event of a specific namespace.
   *
   * Returns an unsubscribe function that can be called to remove the listener
   * and stop message processing. Calling it multiple times has no additional effect
   * after the first call.
   *
   * @param namespace - Bounded context or service name, e.g. `"Billing"`.
   * @param name - Event name, e.g. `"InvoiceCreated"`.
   * @param handler - Handler function that processes the event payload.
   * @param options - Optional configuration (autoAck, etc.).
   * @returns A promise that resolves with an unsubscribe function.
   *
   * @example
   * ```typescript
   * await bus.addEventListener(
   *   'Billing.Invoice',
   *   'InvoiceCreated',
   *   async (message, context) => {
   *     console.log('New invoice:', message);
   *   }
   * );
   * ```
   */
  addEventListener(
    namespace: string,
    name: string,
    handler: BaseMessageHandler,
    options?: ListenerOptions
  ): Promise<Unsubscribe>;

  /**
   * Register a handler for queries of a specific namespace and name.
   *
   * Returns an unsubscribe function that can be called to remove the listener
   * and stop message processing. Calling it multiple times has no additional effect
   * after the first call.
   *
   * @param namespace - Bounded context or service name, e.g. `"Billing"`.
   * @param name - Query name, e.g. `"GetInvoice"`.
   * @param handler - Handler function that processes the query and returns a result.
   * @param options - Optional configuration (autoAck, etc.).
   * @returns A promise that resolves with an unsubscribe function.
   *
   * @example
   * ```typescript
   * await bus.addQueryListener(
   *   'Billing.Invoice',
   *   'GetInvoice',
   *   async (message, context) => {
   *     return { id: message.id, status: 'paid' };
   *   }
   * );
   * ```
   */
  addQueryListener(
    namespace: string,
    name: string,
    handler: BaseMessageHandler,
    options?: ListenerOptions
  ): Promise<Unsubscribe>;
}
