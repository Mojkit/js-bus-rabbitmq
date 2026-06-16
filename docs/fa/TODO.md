I'm developing a new framework in Typescript that name is "wave".
The principles are: microservice, DDD, CQRS, EventSourcing.
The communication between services are by message brokers like rabbitmq;
and this package is a rabbitmq driver for this purpose.
In Wave (according to CQRS) we have some commands,
each command handler can publish one or many events, or reject the commands.
The cammand caller can await for command response (RPC).
In other side, each service can query other services (query side of CQRS).
I want to develop this project to satisfy my needed as described.
The Wave will use in enterprise level projects, with many services and clients.
See the project and give me some insight and advise to enhance the project.
