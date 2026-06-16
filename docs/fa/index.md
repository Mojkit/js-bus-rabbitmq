# درایور ربیت‌ام‌کیو

## اصطلاحات

- queue: یک صف که پیام‌ها از طریق آن انتقال پیدا می‌کنند.

## موارد استفاده

- ایجاد صف
- ارسال و دریافت پیام در کانال send/receive
- ارسال و دریافت پیام در کانال publish/subscribe
- حذف listener
- ارسال ack
- تجمیع و تفکیک صف‌ها. بشه برای چند صف، یک صف واحد در نظر گرفت، و اگه خواستیم جداش کنیم

## نحوه استفاده

### مقدار دهی اولیه

```ts
const connection = {
    host: 'localhost',
    port: '5672',
    user: 'user',
    pass: 'pass'
}
const config = {
    prefix: '',
    separator: '.',
    log: {
        error: console.log,
        warn: console.log,
        info: console.log,
        debug: console.log,
        silly: console.log
    },
    logLevel: 'info',
    rabbitmq: {
        exchangeName: '/',
        prefetch: 2
    }
}
const bus = new Bus(connection, config)
```

### send, listen

این پیام‌ها فقط توسط یک گیرنده دریافت می‌شوند.

```ts
// sender
const header = {
    src: 'Source', // this service! the message are send from this service.
    dest: 'Destination', // that service! the message will be listened in that service.
    type: 'message type',
    name: 'message name',
    optoins: {}, // mesasage type options
    metadata: {}, // other custom data
    bus: { // bus options
        rabbitmq: {} // rabbitmq specific options        
    }
}
const data = {}

await bus.send(header, data)
```

```ts
// listener
const header = {
    dest: 'Destination', // this service is destination of the message. we dont care about source.
    type: 'message type',
    name: 'message name',
    optoins: {}, // mesasage type options
    bus: { // bus options
        rabbitmq: {} // rabbitmq specific options        
    }
}
const callback = () => {}

await bus.listen(header, callback)
```

### publish, subscribe

این پیام توسط چند گیرنده دریافت خواهد شد.

```ts
// publisher
const header = {
    src: 'Source', // this service! the message are publish from this service.
    dest: 'Destination', // that service! the message will be recieved in that service.
    type: 'message type',
    name: 'message name',
    optoins: {}, // mesasage type options
    metadata: {}, // other custom data
    bus: { // bus options
        rabbitmq: {} // rabbitmq specific options        
    }
}
const data = {}

await bus.publish(header, data)
```

```ts
// subscriber
const header = {
    dest: 'Destination', // this service is destination of the message. we dont care about source.
    type: 'message type',
    name: 'message name',
    optoins: {}, // mesasage type options
    metadata: {}, // other custom data
    bus: { // bus options
        rabbitmq: {} // rabbitmq specific options        
    }
}
const callback = () => {}

await bus.publish(header, callback)
```
