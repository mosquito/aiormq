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
   :target: https://pypi.python.org/pypi/aiormq/


aiormq is a pure python AMQP client library.


Status
------

Development - BETA


Features
--------

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
