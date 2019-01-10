======
AIORMQ
======

.. image:: https://coveralls.io/repos/github/mosquito/aiormq/badge.svg?branch=master
   :target: https://coveralls.io/github/mosquito/aiormq?branch=master
   :alt: Coveralls

.. image:: https://img.shields.io/pypi/status/aiormq.svg
   :target: https://github.com/mosquito/aiormq
   :alt: Status

.. image:: https://cloud.drone.io/api/badges/mosquito/aiormq/status.svg
   :target: https://cloud.drone.io/mosquito/aiormq
   :alt: Drone CI

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

Development - BETA


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
* Full SSL/TLS support
* Python `type hints`_
* Uses `pamqp`_ as an AMQP 0.9.1 frame encoder/decoder


.. _Publisher confirms: https://www.rabbitmq.com/confirms.html
.. _Transactions: https://www.rabbitmq.com/semantics.html
.. _PLAIN: https://www.rabbitmq.com/authentication.html
.. _type hints: https://docs.python.org/3/library/typing.html
.. _pamqp: https://pypi.org/project/pamqp/

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
        print(" [x] Received message %r" % message)
        print("Message body is: %r" % message.body)
        print("Before sleep!")
        await asyncio.sleep(5)   # Represents async I/O operations
        print("After sleep!")


    async def main():
        # Perform connection
        connection = await aiormq.connect("amqp://guest:guest@localhost/")

        # Creating a channel
        channel = await connection.channel()

        # Declaring queue
        deaclare_ok = await channel.queue_declare('helo')
        consume_ok = await channel.basic_consume(
            deaclare_ok.queue, on_message, no_ack=True
        )


    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.run_forever()


Simple publisher
****************

.. code-block:: python

    import asyncio
    import aiormq


    async def main():
        # Perform connection
        connection = await aiormq.connect("amqp://guest:guest@localhost/")

        # Creating a channel
        channel = await connection.channel()

        # Sending the message
        await channel.basic_publish(b'Hello World!', routing_key='hello')
        print(" [x] Sent 'Hello World!'")


    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())


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

        print(" [x] Sent %r" % body)

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
        print(" [x] Received message %r" % (message,))
        print("     Message body is: %r" % (message.body,))


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

        print(" [x] Sent %r" % (body,))

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
        print("[x] %r" % (message.body,))

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
        print(" [x] %r:%r" % (message.delivery.routing_key, message.body))
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
            sys.stderr.write(
                "Usage: %s [info] [warning] [error]\n" % sys.argv[0]
            )
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

        print(" [x] Sent %r" % body)

        await connection.close()


    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

