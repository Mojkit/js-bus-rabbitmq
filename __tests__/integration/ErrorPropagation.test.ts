import { test, expect, describe, beforeAll, afterAll } from 'bun:test';
import { RabbitMQWaveTransport } from '../../src/RabbitMQWaveTransport';
import type { WaveCommandMessage, WaveQueryMessage } from '../../src/WaveTransport';

describe('Error Propagation - RPC Pattern', () => {
  let transport: RabbitMQWaveTransport;

  beforeAll(async () => {
    transport = new RabbitMQWaveTransport({
      url: process.env.RABBITMQ_URL ?? 'amqp://guest:guest@localhost:5672',
      prefetchCount: 2,
    });
    await transport.ensureConnected();
  });

  afterAll(async () => {
    await transport.shutdown();
  });

  test('command handler error is propagated to caller', async () => {
    const namespace = 'ErrorTest';
    const commandName = 'FailingCommand';

    // Register a command handler that throws an error
    await transport.addCommandListener(
      namespace,
      commandName,
      async (message) => {
        const error = new Error('Command execution failed');
        (error as any).code = 'COMMAND_FAILED';
        (error as any).context = { reason: 'validation_error', field: 'email' };
        throw error;
      },
      { autoAck: false }
    );

    // Send command and expect error to be thrown
    const command: WaveCommandMessage = {
      kind: 'command',
      namespace,
      name: commandName,
      payload: { test: 'data' },
      awaitResponse: true,
    };

    try {
      await transport.sendCommand(command);
      expect.unreachable('Should have thrown an error');
    } catch (error: any) {
      expect(error).toBeDefined();
      expect(error.message).toBe('Command execution failed');
      expect(error.type).toBe('Error');
      expect(error.code).toBe('COMMAND_FAILED');
      expect(error.context).toEqual({ reason: 'validation_error', field: 'email' });
    }
  });

  test('query handler error is propagated to caller', async () => {
    const namespace = 'ErrorTest';
    const queryName = 'FailingQuery';

    // Register a query handler that throws an error
    await transport.addQueryListener(
      namespace,
      queryName,
      async (message) => {
        const error = new Error('Query execution failed');
        (error as any).code = 'QUERY_FAILED';
        (error as any).context = { reason: 'not_found', id: message.id };
        throw error;
      },
      { autoAck: false }
    );

    // Send query and expect error to be thrown
    const query: WaveQueryMessage = {
      kind: 'query',
      namespace,
      name: queryName,
      payload: { id: '123' },
    };

    try {
      await transport.sendQuery(query);
      expect.unreachable('Should have thrown an error');
    } catch (error: any) {
      expect(error).toBeDefined();
      expect(error.message).toBe('Query execution failed');
      expect(error.type).toBe('Error');
      expect(error.code).toBe('QUERY_FAILED');
      expect(error.context).toEqual({ reason: 'not_found', id: '123' });
    }
  });

  test('successful command response does not throw', async () => {
    const namespace = 'ErrorTest';
    const commandName = 'SuccessfulCommand';

    // Register a command handler that returns successfully
    await transport.addCommandListener(
      namespace,
      commandName,
      async (message) => {
        return { success: true, data: message };
      },
      { autoAck: true }
    );

    // Send command and expect success
    const command: WaveCommandMessage = {
      kind: 'command',
      namespace,
      name: commandName,
      payload: { test: 'data' },
      awaitResponse: true,
    };

    const result = await transport.sendCommand(command);
    expect(result).toEqual({ success: true, data: { test: 'data' } });
  });

  test('error stack trace is included in development mode', async () => {
    const originalEnv = process.env.NODE_ENV;
    process.env.NODE_ENV = 'development';

    const namespace = 'ErrorTest';
    const commandName = 'StackTraceCommand';

    await transport.addCommandListener(
      namespace,
      commandName,
      async () => {
        throw new Error('Error with stack trace');
      },
      { autoAck: false }
    );

    const command: WaveCommandMessage = {
      kind: 'command',
      namespace,
      name: commandName,
      payload: {},
      awaitResponse: true,
    };

    try {
      await transport.sendCommand(command);
      expect.unreachable('Should have thrown an error');
    } catch (error: any) {
      expect(error.remoteStack).toBeDefined();
      expect(error.remoteStack).toContain('Error with stack trace');
    } finally {
      process.env.NODE_ENV = originalEnv;
    }
  });
});
