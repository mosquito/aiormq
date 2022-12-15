======
AIORMQ
======

.. image:: https://coveralls.io/repos/github/mosquito/aiormq/badge.svg?branch=master
   :target: https://coveralls.io/github/mosquito/aiormq?branch=master
   :alt: Coveralls

.. image:: https://img.shields.io/pypi/status/aiormq.svg
   :target: https://github.com/mosquito/aiormq
   :alt: Status

.. image:: https://github.com/mosquito/aiormq/workflows/tox/badge.svg
   :target: https://github.com/mosquito/aiormq/actions?query=workflow%3Atox
   :alt: Build status

.. image:: https://img.shields.io/pypi/v/aiormq.svg
   :target: https://pypi.python.org/pypi/aiormq/
   :alt: Latest Version

.. image:: https://img.shields.io/pypi/wheel/aiormq.svg
   :target: https://pypi.python.org/pypi/aiormq/

.. image:: https://img.shields.io/pypi/pyversions/aiormq.svg
   :target: https://pypi.python.org/pypi/aiormq/

.. image:: https://img.shields.io/pypi/l/aiormq.svg
   :target: https://github.com/mosquito/aiormq/blob/master/LICENSE.md


aiormq is a pure python AMQP client library.

.. contents:: Table of contents

Status
======

* 3.x.x branch - Production/Stable
* 4.x.x branch - Unstable (Experimental)
* 5.x.x and greater is only Production/Stable releases.

Features
========

* Connecting by URL

 * amqp example: **amqp://user:password@server.host/vhost**
 * secure amqp example: **amqps://user:password@server.host/vhost?cafile=ca.pem&keyfile=key.pem&certfile=cert.pem&no_verify_ssl=0**

* Buffered queue for received frames
* Only `PLAIN`_ auth mechanism support
* `Publisher confirms`_ support
* `Transactions`_ support
* Channel based asynchronous locks

  .. note::
      AMQP 0.9.1 requires serialize sending for some frame types
      on the channel. e.g. Content body must be following after
      content header. But frames might be sent asynchronously
      on another channels.

* Tracking unroutable messages
  (Use **connection.channel(on_return_raises=False)** for disabling)
* Full SSL/TLS support, using your choice of:
    * ``amqps://`` url query parameters:
        * ``cafile=`` - string contains path to ca certificate file
        * ``capath=`` - string contains path to ca certificates
        * ``cadata=`` - base64 encoded ca certificate data
        * ``keyfile=`` - string contains path to key file
        * ``certfile=`` - string contains path to certificate file
        * ``no_verify_ssl`` - boolean disables certificates validation
    * ``context=`` `SSLContext`_ keyword argument to ``connect()``.
* Python `type hints`_
* Uses `pamqp`_ as an AMQP 0.9.1 frame encoder/decoder


.. _Publisher confirms: https://www.rabbitmq.com/confirms.html
.. _Transactions: https://www.rabbitmq.com/semantics.html
.. _PLAIN: https://www.rabbitmq.com/authentication.html
.. _type hints: https://docs.python.org/3/library/typing.html
.. _pamqp: https://pypi.org/project/pamqp/
.. _SSLContext: https://docs.python.org/3/library/ssl.html#ssl.SSLContext

Tutorial
========

Introduction
------------

Simple consumer
***************

.. code-block:: python

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
        await asyncio.sleep(5)   # Represents async I/O operations
        print("After sleep!")


    async def main():
        # Perform connection
        connection = await aiormq.connect("amqp://guest:guest@localhost/")

        # Creating a channel
        channel = await connection.channel()

        # Declaring queue
        declare_ok = await channel.queue_declare('helo')
        consume_ok = await channel.basic_consume(
            declare_ok.queue, on_message, no_ack=True
        )


    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.run_forever()


Simple publisher
****************

.. code-block:: python
    :name: test_simple_publisher

    import asyncio
    from typing import Optional

    import aiormq
    from aiormq.abc import DeliveredMessage


    MESSAGE: Optional[DeliveredMessage] = None


    async def main():
        global MESSAGE

        body = b'Hello World!'

        # Perform connection
        connection = await aiormq.connect("amqp://guest:guest@localhost//")

        # Creating a channel
        channel = await connection.channel()

        declare_ok = await channel.queue_declare("hello", auto_delete=True)

        # Sending the message
        await channel.basic_publish(body, routing_key='hello')
        print(f" [x] Sent {body}")

        MESSAGE = await channel.basic_get(declare_ok.queue)
        print(f" [x] Received message from {declare_ok.queue!r}")


    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

    assert MESSAGE is not None
    assert MESSAGE.routing_key == "hello"
    assert MESSAGE.body == b'Hello World!'


Work Queues
-----------

Create new task
***************

.. code-block:: python

    import sys
    import asyncio
    import aiormq


    async def main():
        # Perform connection
        connection = await aiormq.connect("amqp://guest:guest@localhost/")

        # Creating a channel
        channel = await connection.channel()

        body = b' '.join(sys.argv[1:]) or b"Hello World!"

        # Sending the message
        await channel.basic_publish(
            body,
            routing_key='task_queue',
            properties=aiormq.spec.Basic.Properties(
                delivery_mode=1,
            )
        )

        print(f" [x] Sent {body!r}")

        await connection.close()


    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())


Simple worker
*************

.. code-block:: python

    import asyncio
    import aiormq
    import aiormq.types


    async def on_message(message: aiormq.types.DeliveredMessage):
        print(f" [x] Received message {message!r}")
        print(f"     Message body is: {message.body!r}")


    async def main():
        # Perform connection
        connection = await aiormq.connect("amqp://guest:guest@localhost/")


        # Creating a channel
        channel = await connection.channel()
        await channel.basic_qos(prefetch_count=1)

        # Declaring queue
        declare_ok = await channel.queue_declare('task_queue', durable=True)

        # Start listening the queue with name 'task_queue'
        await channel.basic_consume(declare_ok.queue, on_message, no_ack=True)


    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

    # we enter a never-ending loop that waits for data and runs
    # callbacks whenever necessary.
    print(" [*] Waiting for messages. To exit press CTRL+C")
    loop.run_forever()


Publish Subscribe
-----------------

Publisher
*********

.. code-block:: python

    import sys
    import asyncio
    import aiormq


    async def main():
        # Perform connection
        connection = await aiormq.connect("amqp://guest:guest@localhost/")

        # Creating a channel
        channel = await connection.channel()

        await channel.exchange_declare(
            exchange='logs', exchange_type='fanout'
        )

        body = b' '.join(sys.argv[1:]) or b"Hello World!"

        # Sending the message
        await channel.basic_publish(
            body, routing_key='info', exchange='logs'
        )

        print(f" [x] Sent {body!r}")

        await connection.close()


    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())


Subscriber
**********

.. code-block:: python

    import asyncio
    import aiormq
    import aiormq.types


    async def on_message(message: aiormq.types.DeliveredMessage):
        print(f"[x] {message.body!r}")

        await message.channel.basic_ack(
            message.delivery.delivery_tag
        )


    async def main():
        # Perform connection
        connection = await aiormq.connect("amqp://guest:guest@localhost/")

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

        # Start listening the queue with name 'task_queue'
        await channel.basic_consume(declare_ok.queue, on_message)


    loop = asyncio.get_event_loop()
    loop.create_task(main())

    # we enter a never-ending loop that waits for data
    # and runs callbacks whenever necessary.
    print(' [*] Waiting for logs. To exit press CTRL+C')
    loop.run_forever()


Routing
-------

Direct consumer
***************

.. code-block:: python

    import sys
    import asyncio
    import aiormq
    import aiormq.types


    async def on_message(message: aiormq.types.DeliveredMessage):
        print(f" [x] {message.delivery.routing_key!r}:{message.body!r}"
        await message.channel.basic_ack(
            message.delivery.delivery_tag
        )


    async def main():
        # Perform connection
        connection = aiormq.Connection("amqp://guest:guest@localhost/")
        await connection.connect()

        # Creating a channel
        channel = await connection.channel()
        await channel.basic_qos(prefetch_count=1)

        severities = sys.argv[1:]

        if not severities:
            sys.stderr.write(f"Usage: {sys.argv[0]} [info] [warning] [error]\n")
            sys.exit(1)

        # Declare an exchange
        await channel.exchange_declare(
            exchange='logs', exchange_type='direct'
        )

        # Declaring random queue
        declare_ok = await channel.queue_declare(durable=True, auto_delete=True)

        for severity in severities:
            await channel.queue_bind(
                declare_ok.queue, 'logs', routing_key=severity
            )

        # Start listening the random queue
        await channel.basic_consume(declare_ok.queue, on_message)


    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

    # we enter a never-ending loop that waits for data
    # and runs callbacks whenever necessary.
    print(" [*] Waiting for messages. To exit press CTRL+C")
    loop.run_forever()


Emitter
*******

.. code-block:: python

    import sys
    import asyncio
    import aiormq


    async def main():
        # Perform connection
        connection = await aiormq.connect("amqp://guest:guest@localhost/")

        # Creating a channel
        channel = await connection.channel()

        await channel.exchange_declare(
            exchange='logs', exchange_type='direct'
        )

        body = (
            b' '.join(arg.encode() for arg in sys.argv[2:])
            or
            b"Hello World!"
        )

        # Sending the message
        routing_key = sys.argv[1] if len(sys.argv) > 2 else 'info'

        await channel.basic_publish(
            body, exchange='logs', routing_key=routing_key,
            properties=aiormq.spec.Basic.Properties(
                delivery_mode=1
            )
        )

        print(f" [x] Sent {body!r}")

        await connection.close()


    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

Topics
------

Publisher
*********

.. code-block:: python

    import sys
    import asyncio
    import aiormq


    async def main():
        # Perform connection
        connection = await aiormq.connect("amqp://guest:guest@localhost/")

        # Creating a channel
        channel = await connection.channel()

        await channel.exchange_declare('topic_logs', exchange_type='topic')

        routing_key = (
            sys.argv[1] if len(sys.argv) > 2 else 'anonymous.info'
        )

        body = (
            b' '.join(arg.encode() for arg in sys.argv[2:])
            or
            b"Hello World!"
        )

        # Sending the message
        await channel.basic_publish(
            body, exchange='topic_logs', routing_key=routing_key,
            properties=aiormq.spec.Basic.Properties(
                delivery_mode=1
            )
        )

        print(f" [x] Sent {body!r}")

        await connection.close()


    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

Consumer
********

.. code-block:: python

    import asyncio
    import sys
    import aiormq
    import aiormq.types


    async def on_message(message: aiormq.types.DeliveredMessage):
        print(f" [x] {message.delivery.routing_key!r}:{message.body!r}")
        await message.channel.basic_ack(
            message.delivery.delivery_tag
        )


    async def main():
        # Perform connection
        connection = await aiormq.connect(
            "amqp://guest:guest@localhost/", loop=loop
        )

        # Creating a channel
        channel = await connection.channel()
        await channel.basic_qos(prefetch_count=1)

        # Declare an exchange
        await channel.exchange_declare('topic_logs', exchange_type='topic')

        # Declaring queue
        declare_ok = await channel.queue_declare('task_queue', durable=True)

        binding_keys = sys.argv[1:]

        if not binding_keys:
            sys.stderr.write(
                f"Usage: {sys.argv[0]} [binding_key]...\n"
            )
            sys.exit(1)

        for binding_key in binding_keys:
            await channel.queue_bind(
                declare_ok.queue, 'topic_logs', routing_key=binding_key
            )

        # Start listening the queue with name 'task_queue'
        await channel.basic_consume(declare_ok.queue, on_message)


    loop = asyncio.get_event_loop()
    loop.create_task(main())

    # we enter a never-ending loop that waits for
    # data and runs callbacks whenever necessary.
    print(" [*] Waiting for messages. To exit press CTRL+C")
    loop.run_forever()

Remote procedure call (RPC)
---------------------------

RPC server
**********

.. code-block:: python

    import asyncio
    import aiormq
    import aiormq.types


    def fib(n):
        if n == 0:
            return 0
        elif n == 1:
            return 1
        else:
            return fib(n-1) + fib(n-2)


    async def on_message(message:aiormq.types.DeliveredMessage):
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


    async def main():
        # Perform connection
        connection = await aiormq.connect("amqp://guest:guest@localhost/")

        # Creating a channel
        channel = await connection.channel()

        # Declaring queue
        declare_ok = await channel.queue_declare('rpc_queue')

        # Start listening the queue with name 'hello'
        await channel.basic_consume(declare_ok.queue, on_message)


    loop = asyncio.get_event_loop()
    loop.create_task(main())

    # we enter a never-ending loop that waits for data
    # and runs callbacks whenever necessary.
    print(" [x] Awaiting RPC requests")
    loop.run_forever()


RPC client
**********

.. code-block:: python

    import asyncio
    import uuid
    import aiormq
    import aiormq.types


    class FibonacciRpcClient:
        def __init__(self):
            self.connection = None      # type: aiormq.Connection
            self.channel = None         # type: aiormq.Channel
            self.callback_queue = ''
            self.futures = {}
            self.loop = loop

        async def connect(self):
            self.connection = await aiormq.connect("amqp://guest:guest@localhost/")

            self.channel = await self.connection.channel()
            declare_ok = await self.channel.queue_declare(
                exclusive=True, auto_delete=True
            )

            await self.channel.basic_consume(declare_ok.queue, self.on_response)

            self.callback_queue = declare_ok.queue

            return self

        async def on_response(self, message: aiormq.types.DeliveredMessage):
            future = self.futures.pop(message.header.properties.correlation_id)
            future.set_result(message.body)

        async def call(self, n):
            correlation_id = str(uuid.uuid4())
            future = loop.create_future()

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


    async def main():
        fibonacci_rpc = await FibonacciRpcClient().connect()
        print(" [x] Requesting fib(30)")
        response = await fibonacci_rpc.call(30)
        print(r" [.] Got {response!r}")


    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
