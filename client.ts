import createBus from './src/index';

async function main() {
  const bus = createBus();

  // Fire-and-forget event
  await bus.sendEvent({
    kind: 'event',
    namespace: 'MyContext.MyAggregate',
    name: 'UserVisited',
    payload: { id: 1, name: 'Alan Turing' },
  });

  // Fire-and-forget command
  await bus.sendCommand({
    kind: 'command',
    namespace: 'MyContext.MyAggregate',
    name: 'DoSomething',
    payload: { foo: 'bar' },
  });

  // Query with RPC-style response
  const result = await bus.sendQuery<{ id: number }, { name: string }>({
    kind: 'query',
    namespace: 'MyContext.MyAggregate',
    name: 'GetUser',
    payload: { id: 1 },
  });

  // eslint-disable-next-line no-console
  console.log('query result', result);
}

main().catch((err) => {
  // eslint-disable-next-line no-console
  console.error(err);
  process.exit(1);
});