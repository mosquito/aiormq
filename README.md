# AIORMQ

[![Coveralls](https://coveralls.io/repos/github/mosquito/aiormq/badge.svg?branch=master)](https://coveralls.io/github/mosquito/aiormq?branch=master)
[![Status](https://img.shields.io/pypi/status/aiormq.svg)](https://github.com/mosquito/aiormq)
[![Build status](https://github.com/mosquito/aiormq/workflows/tests/badge.svg)](https://github.com/mosquito/aiormq/actions?query=workflow%3Atests)
[![Latest Version](https://img.shields.io/pypi/v/aiormq.svg)](https://pypi.python.org/pypi/aiormq/)
[![Wheel](https://img.shields.io/pypi/wheel/aiormq.svg)](https://pypi.python.org/pypi/aiormq/)
[![Python versions](https://img.shields.io/pypi/pyversions/aiormq.svg)](https://pypi.python.org/pypi/aiormq/)
[![License](https://img.shields.io/pypi/l/aiormq.svg)](https://github.com/mosquito/aiormq/blob/master/LICENSE.md)

aiormq is a pure python AMQP client library.

## Table of contents

* [Status](#status)
* [Features](#features)
* [Tutorial](#tutorial)
  * [Introduction](#introduction)
  * [Work Queues](#work-queues)
  * [Publish Subscribe](#publish-subscribe)
  * [Routing](#routing)
  * [Topics](#topics)
  * [Consumer cancelled by the broker](#consumer-cancelled-by-the-broker)
  * [Remote procedure call (RPC)](#remote-procedure-call-rpc)

## Status

* 3.x.x branch - Production/Stable
* 4.x.x branch - Unstable (Experimental)
* 5.x.x and greater is only Production/Stable releases.

## Features

* Connecting by URL

  * amqp example: **amqp://user:password@server.host/vhost**
  * secure amqp example: **amqps://user:password@server.host/vhost?cafile=ca.pem&keyfile=key.pem&certfile=cert.pem&no_verify_ssl=0**

* Buffered queue for received frames
* Only [PLAIN](https://www.rabbitmq.com/authentication.html) auth mechanism support
* [Publisher confirms](https://www.rabbitmq.com/confirms.html) support
* [Transactions](https://www.rabbitmq.com/semantics.html) support
* Channel based asynchronous locks

  > **Note**
  > AMQP 0.9.1 requires serialize sending for some frame types
  > on the channel. e.g. Content body must be following after
  > content header. But frames might be sent asynchronously
  > on another channels.

* Tracking unroutable messages
  (Use **connection.channel(on_return_raises=False)** for disabling)
* Full SSL/TLS support, using your choice of:
  * `amqps://` url query parameters:
    * `cafile=` - string contains path to ca certificate file
    * `capath=` - string contains path to ca certificates
    * `cadata=` - base64 encoded ca certificate data
    * `keyfile=` - string contains path to key file
    * `certfile=` - string contains path to certificate file
    * `no_verify_ssl` - boolean disables certificates validation
  * `context=` [SSLContext](https://docs.python.org/3/library/ssl.html#ssl.SSLContext) keyword argument to `connect()`.
* Python [type hints](https://docs.python.org/3/library/typing.html)
* Uses [pamqp](https://pypi.org/project/pamqp/) as an AMQP 0.9.1 frame encoder/decoder

## Tutorial

In the examples below `amqp_url` is a connection URL string such as
`amqp://guest:guest@localhost/`. The examples run inside a coroutine, so
`await` is used at the top level.

`aiormq.connect()` prepares a connection without opening it.
`async with aiormq.connect(url) as connection:` opens the connection and
closes it on exit. `await aiormq.connect(url)` from older versions still
works.

### Introduction

#### Simple consumer

<!-- name: async test_simple_consumer; fixtures: amqp_url, wait_for_output -->
```python
import asyncio
import aiormq


async def on_message(message):
    """
    on_message doesn't necessarily have to be defined as async.
    Here it is to show that it's possible.
    """
    print(f" [x] Received message {message!r}")
    print(f"Message body is: {message.body!r}")
    print("Before sleep!")
    await asyncio.sleep(1)   # Represents async I/O operations
    print("After sleep!")


# Perform connection
async with aiormq.connect(amqp_url) as connection:
    # Creating a channel
    channel = await connection.channel()

    # Declaring queue
    declare_ok = await channel.queue_declare('hello', auto_delete=True)
    consume_ok = await channel.basic_consume(
        declare_ok.queue, on_message, no_ack=True
    )
    # The connection stays open while this block runs.
```
<!--
name: test_simple_consumer
```python
    await channel.basic_publish(b"Hello World!", routing_key=declare_ok.queue)
    await wait_for_output("After sleep!")
```
-->

#### Simple publisher

<!-- name: async test_simple_publisher; fixtures: amqp_url -->
```python
import aiormq

body = b'Hello World!'

# Perform connection
async with aiormq.connect(amqp_url) as connection:
    # Creating a channel
    channel = await connection.channel()

    declare_ok = await channel.queue_declare("hello", auto_delete=True)

    # Sending the message
    await channel.basic_publish(body, routing_key='hello')
    print(f" [x] Sent {body}")

    message = await channel.basic_get(declare_ok.queue)
    print(f" [x] Received message from {declare_ok.queue!r}")

    assert message is not None
    assert message.routing_key == "hello"
    assert message.body == b'Hello World!'
```

### Work Queues

#### Create new task

<!--
name: async test_work_queues_new_task;
fixtures: amqp_url
```python
import aiormq

# The worker declares the durable queue. Declare it here too, so the
# task is not lost when no worker runs yet.
setup_connection = aiormq.Connection(amqp_url)
await setup_connection.connect()
setup_channel = await setup_connection.channel()
await setup_channel.queue_declare('task_queue', durable=True)
```
-->
```python
import aiormq

# Perform connection
async with aiormq.connect(amqp_url) as connection:
    # Creating a channel
    channel = await connection.channel()

    body = b"Hello World!"

    # Sending the message
    await channel.basic_publish(
        body,
        routing_key='task_queue',
        properties=aiormq.spec.Basic.Properties(
            delivery_mode=1,
        )
    )

    print(f" [x] Sent {body!r}")
```
<!--
name: test_work_queues_new_task
```python
message = await setup_channel.basic_get('task_queue', no_ack=True)
assert message.body == body
await setup_channel.queue_delete('task_queue')
await setup_connection.close()
```
-->

#### Simple worker

<!-- name: async test_work_queues_worker; fixtures: amqp_url, wait_for_output -->
```python
import aiormq
import aiormq.abc


async def on_message(message: aiormq.abc.DeliveredMessage):
    print(f" [x] Received message {message!r}")
    print(f"     Message body is: {message.body!r}")


# Perform connection
async with aiormq.connect(amqp_url) as connection:
    # Creating a channel
    channel = await connection.channel()
    await channel.basic_qos(prefetch_count=1)

    # Declaring queue
    declare_ok = await channel.queue_declare('task_queue', durable=True)

    # Start listening the queue with name 'task_queue'
    await channel.basic_consume(declare_ok.queue, on_message, no_ack=True)

    print(" [*] Waiting for messages.")
    # The connection stays open while this block runs.
```
<!--
name: test_work_queues_worker
```python
    await channel.basic_publish(b"task", routing_key='task_queue')
    await wait_for_output("Message body is: b'task'")
    await channel.queue_delete('task_queue')
```
-->

### Publish Subscribe

#### Publisher

<!-- name: async test_publish_subscribe_publisher; fixtures: amqp_url -->
```python
import aiormq

# Perform connection
async with aiormq.connect(amqp_url) as connection:
    # Creating a channel
    channel = await connection.channel()

    await channel.exchange_declare(
        exchange='logs', exchange_type='fanout'
    )

    body = b"Hello World!"

    # Sending the message
    await channel.basic_publish(
        body, routing_key='info', exchange='logs'
    )

    print(f" [x] Sent {body!r}")
```

#### Subscriber

<!-- name: async test_publish_subscribe_subscriber; fixtures: amqp_url, wait_for_output -->
```python
import aiormq
import aiormq.abc


async def on_message(message: aiormq.abc.DeliveredMessage):
    print(f"[x] {message.body!r}")

    await message.channel.basic_ack(
        message.delivery.delivery_tag
    )


# Perform connection
async with aiormq.connect(amqp_url) as connection:
    # Creating a channel
    channel = await connection.channel()
    await channel.basic_qos(prefetch_count=1)

    await channel.exchange_declare(
        exchange='logs', exchange_type='fanout'
    )

    # Declaring queue
    declare_ok = await channel.queue_declare(exclusive=True)

    # Binding the queue to the exchange
    await channel.queue_bind(declare_ok.queue, 'logs')

    # Start listening the queue
    await channel.basic_consume(declare_ok.queue, on_message)

    print(' [*] Waiting for logs.')
    # The connection stays open while this block runs.
```
<!--
name: test_publish_subscribe_subscriber
```python
    await channel.basic_publish(b"log line", routing_key='info', exchange='logs')
    await wait_for_output("[x] b'log line'")
    await channel.exchange_delete('logs')
```
-->

### Routing

#### Direct consumer

<!-- name: async test_routing_direct_consumer; fixtures: amqp_url, wait_for_output -->
```python
import aiormq
import aiormq.abc


async def on_message(message: aiormq.abc.DeliveredMessage):
    print(f" [x] {message.delivery.routing_key!r}:{message.body!r}")
    await message.channel.basic_ack(
        message.delivery.delivery_tag
    )


# Perform connection
async with aiormq.Connection(amqp_url) as connection:
    # Creating a channel
    channel = await connection.channel()
    await channel.basic_qos(prefetch_count=1)

    severities = ["info", "warning", "error"]

    # Declare an exchange
    await channel.exchange_declare(
        exchange='direct_logs', exchange_type='direct'
    )

    # Declaring random queue
    declare_ok = await channel.queue_declare(durable=True, auto_delete=True)

    for severity in severities:
        await channel.queue_bind(
            declare_ok.queue, 'direct_logs', routing_key=severity
        )

    # Start listening the random queue
    await channel.basic_consume(declare_ok.queue, on_message)

    print(" [*] Waiting for messages.")
    # The connection stays open while this block runs.
```
<!--
name: test_routing_direct_consumer
```python
    await channel.basic_publish(
        b"disk full", routing_key='error', exchange='direct_logs',
    )
    await wait_for_output("[x] 'error':b'disk full'")
    await channel.exchange_delete('direct_logs')
```
-->

#### Emitter

<!-- name: async test_routing_emitter; fixtures: amqp_url -->
```python
import aiormq

# Perform connection
async with aiormq.connect(amqp_url) as connection:
    # Creating a channel
    channel = await connection.channel()

    await channel.exchange_declare(
        exchange='direct_logs', exchange_type='direct'
    )

    routing_key = 'info'
    body = b"Hello World!"

    # Sending the message
    await channel.basic_publish(
        body, exchange='direct_logs', routing_key=routing_key,
        properties=aiormq.spec.Basic.Properties(
            delivery_mode=1
        )
    )

    print(f" [x] Sent {body!r}")
```

### Topics

#### Publisher

<!-- name: async test_topics_publisher; fixtures: amqp_url -->
```python
import aiormq

# Perform connection
async with aiormq.connect(amqp_url) as connection:
    # Creating a channel
    channel = await connection.channel()

    await channel.exchange_declare('topic_logs', exchange_type='topic')

    routing_key = 'anonymous.info'
    body = b"Hello World!"

    # Sending the message
    await channel.basic_publish(
        body, exchange='topic_logs', routing_key=routing_key,
        properties=aiormq.spec.Basic.Properties(
            delivery_mode=1
        )
    )

    print(f" [x] Sent {body!r}")
```

#### Consumer

<!-- name: async test_topics_consumer; fixtures: amqp_url, wait_for_output -->
```python
import aiormq
import aiormq.abc


async def on_message(message: aiormq.abc.DeliveredMessage):
    print(f" [x] {message.delivery.routing_key!r}:{message.body!r}")
    await message.channel.basic_ack(
        message.delivery.delivery_tag
    )


# Perform connection
async with aiormq.connect(amqp_url) as connection:
    # Creating a channel
    channel = await connection.channel()
    await channel.basic_qos(prefetch_count=1)

    # Declare an exchange
    await channel.exchange_declare('topic_logs', exchange_type='topic')

    # Declaring queue
    declare_ok = await channel.queue_declare(exclusive=True)

    binding_keys = ["*.info", "kern.*"]

    for binding_key in binding_keys:
        await channel.queue_bind(
            declare_ok.queue, 'topic_logs', routing_key=binding_key
        )

    # Start listening the queue
    await channel.basic_consume(declare_ok.queue, on_message)

    print(" [*] Waiting for messages.")
    # The connection stays open while this block runs.
```
<!--
name: test_topics_consumer
```python
    await channel.basic_publish(
        b"critical", routing_key='kern.critical', exchange='topic_logs',
    )
    await wait_for_output("[x] 'kern.critical':b'critical'")
    await channel.exchange_delete('topic_logs')
```
-->

### Consumer cancelled by the broker

The broker cancels a consumer when its queue is deleted or when a cluster
node that hosts the queue goes away. Register a callback in
`channel.on_consumer_cancel_callbacks` to get the `Basic.Cancel` frame and
react, for example by consuming again or by stopping the application.

<!-- name: async test_consumer_cancel_notification; fixtures: amqp_url -->
```python
import asyncio
import aiormq


async def on_message(message):
    print(f" [x] Received message {message.body!r}")


cancelled = asyncio.get_running_loop().create_future()


def on_consumer_cancel(frame: aiormq.spec.Basic.Cancel):
    print(f" [!] Consumer {frame.consumer_tag!r} cancelled by the broker")
    cancelled.set_result(frame.consumer_tag)


async with aiormq.connect(amqp_url) as connection:
    channel = await connection.channel()
    channel.on_consumer_cancel_callbacks.add(on_consumer_cancel)

    declare_ok = await channel.queue_declare('cancel_me', auto_delete=True)
    consume_ok = await channel.basic_consume(declare_ok.queue, on_message)

    # Deleting the queue makes the broker cancel the consumer.
    await channel.queue_delete(declare_ok.queue)

    assert await cancelled == consume_ok.consumer_tag
```

### Remote procedure call (RPC)

#### RPC server

<!-- name: async test_rpc; fixtures: amqp_url -->
```python
import aiormq
import aiormq.abc


def fib(n):
    if n == 0:
        return 0
    elif n == 1:
        return 1
    else:
        return fib(n-1) + fib(n-2)


async def on_message(message: aiormq.abc.DeliveredMessage):
    n = int(message.body.decode())

    print(f" [.] fib({n})")
    response = str(fib(n)).encode()

    await message.channel.basic_publish(
        response, routing_key=message.header.properties.reply_to,
        properties=aiormq.spec.Basic.Properties(
            correlation_id=message.header.properties.correlation_id
        ),

    )

    await message.channel.basic_ack(message.delivery.delivery_tag)
    print('Request complete')


# Perform connection
server_connection = aiormq.Connection(amqp_url)
await server_connection.connect()

# Creating a channel
server_channel = await server_connection.channel()

# Declaring queue
declare_ok = await server_channel.queue_declare('rpc_queue', auto_delete=True)

# Start listening the queue with name 'rpc_queue'
await server_channel.basic_consume(declare_ok.queue, on_message)

print(" [x] Awaiting RPC requests")
```

#### RPC client

<!-- name: test_rpc -->
```python
import asyncio
import uuid
import aiormq
import aiormq.abc


class FibonacciRpcClient:
    def __init__(self):
        self.connection = None      # type: aiormq.Connection
        self.channel = None         # type: aiormq.Channel
        self.callback_queue = ''
        self.futures = {}

    async def connect(self):
        self.connection = aiormq.Connection(amqp_url)
        await self.connection.connect()

        self.channel = await self.connection.channel()
        declare_ok = await self.channel.queue_declare(
            exclusive=True, auto_delete=True
        )

        await self.channel.basic_consume(declare_ok.queue, self.on_response)

        self.callback_queue = declare_ok.queue

        return self

    async def on_response(self, message: aiormq.abc.DeliveredMessage):
        future = self.futures.pop(message.header.properties.correlation_id)
        future.set_result(message.body)

    async def call(self, n):
        correlation_id = str(uuid.uuid4())
        future = asyncio.get_running_loop().create_future()

        self.futures[correlation_id] = future

        await self.channel.basic_publish(
            str(n).encode(), routing_key='rpc_queue',
            properties=aiormq.spec.Basic.Properties(
                content_type='text/plain',
                correlation_id=correlation_id,
                reply_to=self.callback_queue,
            )
        )

        return int(await future)


fibonacci_rpc = await FibonacciRpcClient().connect()
print(" [x] Requesting fib(30)")
response = await fibonacci_rpc.call(30)
print(f" [.] Got {response!r}")

await fibonacci_rpc.connection.close()
```
<!--
name: test_rpc
```python
assert response == 832040
await server_connection.close()
```
-->
