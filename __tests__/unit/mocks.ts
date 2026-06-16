/**
 * Mock implementations for testing RabbitMQ components.
 * These mocks allow isolation of unit tests from external dependencies.
 */

import type { WaveLogger } from '../../src/Logging';
import type { WavePropagator } from '../../src/Propagation';
import type { WaveTracer } from '../../src/Tracing';

/**
 * Tracks calls made to a mock logger.
 */
export interface MockLoggerCalls {
  debug: number;
  info: number;
  warn: number;
  error: number;
  messages: Array<{
    level: 'debug' | 'info' | 'warn' | 'error';
    message: string;
    meta?: Record<string, unknown>;
  }>;
}

export interface TrackedLogger extends WaveLogger {
  calls: MockLoggerCalls;
}

/**
 * Creates a mock logger that tracks all calls for inspection.
 */
export function createMockLogger(): TrackedLogger {
  const calls: MockLoggerCalls = {
    debug: 0,
    info: 0,
    warn: 0,
    error: 0,
    messages: [],
  };

  const mockLogger = {
    debug(message: string, meta?: Record<string, unknown>): void {
      calls.debug++;
      calls.messages.push({ level: 'debug', message, meta });
    },
    info(message: string, meta?: Record<string, unknown>): void {
      calls.info++;
      calls.messages.push({ level: 'info', message, meta });
    },
    warn(message: string, meta?: Record<string, unknown>): void {
      calls.warn++;
      calls.messages.push({ level: 'warn', message, meta });
    },
    error(message: string, meta?: Record<string, unknown>): void {
      calls.error++;
      calls.messages.push({ level: 'error', message, meta });
    },
  };

  // Attach calls tracking to the mock
  const tracked = Object.assign(mockLogger, { calls });
  return tracked;
}

/**
 * Tracks calls made to a mock tracer.
 */
export interface TrackedTracer extends WaveTracer {
  calls: MockTracerCalls;
}

/**
 * Creates a mock tracer that tracks calls and optionally returns a value.
 */
export function createMockTracer(): TrackedTracer {
  const calls: MockTracerCalls = {
    startActiveSpan: 0,
    spanNames: [],
    returnValue: undefined,
  };

  const mockTracer = {
    async startActiveSpan<T>(name: string, fn: () => Promise<T>): Promise<T> {
      calls.startActiveSpan++;
      calls.spanNames.push(name);
      if (calls.returnValue !== undefined) {
        return Promise.resolve(calls.returnValue as T);
      }
      return fn();
    },
    calls,
  };

  return mockTracer;
}

/**
 * Tracks calls made to a mock propagator.
 */
export interface TrackedPropagator extends WavePropagator {
  calls: MockPropagatorCalls;
}

/**
 * Creates a mock propagator that tracks calls and optionally returns a value.
 */
export function createMockPropagator(): TrackedPropagator {
  const calls: MockPropagatorCalls = {
    inject: 0,
    lastReturnValue: undefined,
  };

  const mockPropagator = {
    inject(): Record<string, string> {
      calls.inject++;
      return (calls.lastReturnValue ?? {}) as Record<string, string>;
    },
    calls,
  };

  return mockPropagator;
}
