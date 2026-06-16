/**
 * @file Logging.ts - Wave logging abstraction.
 *
 * This module defines a minimal logging interface and provides two implementations:
 * a no-op logger (for environments where logging is not needed) and a simple
 * console logger for development/debugging.
 *
 * A concrete logger adapter (e.g., Winston, Pino) should live in the base Wave
 * package and be injected into this transport via dependency injection.
 */

/**
 * Minimal logging interface used throughout the Wave bus.
 *
 * All methods accept an optional metadata object that can contain
 * additional structured information (e.g., user IDs, correlation IDs, stack traces).
 *
 * @example
 * ```typescript
 * logger.info('rabbitmq.connection.established', { url: 'amqp://localhost:5672' });
 * logger.error('command.failed', { error: err.message, correlationId: 'abc-123' });
 * ```
 */
export interface WaveLogger {
  /**
   * Log a debug-level message.
   * @param message - A short human-readable message describing the event.
   * @param meta - Optional structured metadata to include with the log entry.
   */
  debug(message: string, meta?: Record<string, unknown>): void;

  /**
   * Log an informational message.
   * @param message - A short human-readable message describing the event.
   * @param meta - Optional structured metadata to include with the log entry.
   */
  info(message: string, meta?: Record<string, unknown>): void;

  /**
   * Log a warning-level message.
   * @param message - A short human-readable message describing the event.
   * @param meta - Optional structured metadata to include with the log entry.
   */
  warn(message: string, meta?: Record<string, unknown>): void;

  /**
   * Log an error-level message.
   * @param message - A short human-readable message describing the error or failure.
   * @param meta - Optional structured metadata to include with the log entry (e.g., stack traces).
   */
  error(message: string, meta?: Record<string, unknown>): void;
}

/**
 * A no-op implementation of {@link WaveLogger} that discards all log messages.
 *
 * This is useful in testing environments or when logging is provided externally.
 */
export class NoopWaveLogger implements WaveLogger {
  debug(): void {}
  info(): void {}
  warn(): void {}
  error(): void {}
}

/**
 * A simple console-based implementation of {@link WaveLogger}.
 *
 * Writes all log levels to `console.log` except `error`, which writes to `console.error`.
 * Intended for development and debugging purposes only; not suitable for production use
 * where a proper structured logger should be used.
 *
 * @example
 * ```typescript
 * const bus = createBus({}, myTracer, new ConsoleWaveLogger(), myPropagator);
 * ```
 */
export class ConsoleWaveLogger implements WaveLogger {
  debug(message: string, meta?: Record<string, unknown>): void {
    console.log(message, meta);
  }
  info(message: string, meta?: Record<string, unknown>): void {
    console.log(message, meta);
  }
  warn(message: string, meta?: Record<string, unknown>): void {
    console.log(message, meta);
  }
  error(message: string, meta?: Record<string, unknown>): void {
    console.error(message, meta);
  }
}
