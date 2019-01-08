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

* Buffered queue for received channels
* `Publisher confirms`_ support
* `Transactions`_ support
* Channel based asynchronous locks

  .. note::
      AMQP 0.9.1 requires serialize sending for some frame types
      on the channel. e.g. Content body must be following after
      content header. But frames might be sent asynchronously
      on another channels.

* Tracking unroutable messages
* Full SSL/TLS support


.. _Publisher confirms: https://www.rabbitmq.com/confirms.html
.. _Transactions: https://www.rabbitmq.com/semantics.html
