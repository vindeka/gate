# Copyright (c) 2013 Vindeka, LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import socket
import threading
import traceback
import uuid
import sys
import time
import logging
import itertools
import inspect

from datetime import datetime, timedelta
from collections import namedtuple
from kombu.connection import Connection
from kombu.messaging import Consumer, Producer
from kombu.pools import connections
from kombu.entity import Queue, Exchange
from gate.common.exceptions import RpcError, BadRequestError, NotFoundError, \
    UnknownOperationError, WriteConflictError
from gate.common.utils import config_true_value

log = logging.getLogger(__name__)

DEFAULT_HEARTBEAT = 30
DEFAULT_QUEUE_EXPIRATION = 60.0


def rpc_method(func):
    func.is_rpc_method = True
    return func


class RpcController(object):

    def __init__(self, conf, name, exchange, logger=None):
        """Set up rpc Controller

        @param conf: configuration object
        @param name: name of the controller
        @param logger: logger to use for controller
        """
        self.conf = conf
        self.logger = logger
        self.broker_url = \
            conf.get('broker_url', 'amqp://guest:guest@localhost:5672//')
        self.use_ssl = \
            config_true_value(conf.get('broker_use_ssl', 'False'))
        self.connection_timeout = \
            int(conf.get('broker_connection_timeout', '5'))

        self.rpc = RpcConnection(name, self.broker_url, exchange,
                                 auto_delete=True, logger=logger)
        for method, func in inspect.getmembers(self, predicate=inspect.ismethod):
            if hasattr(func, "is_rpc_method") and func.is_rpc_method:
                self.rpc.handle(func, method)

    def handle(self, operation, operation_name=None, sender_kwarg=None):
        """Handle an operation using the specified function

        @param operation: function to call for this operation
        @param operation_name: operation name. if unspecifed operation.__name__ is used
        @param sender_kwarg: optional keyword arg on operation to feed in sender name
        """
        self.rpc.handle(operation, operation_name, sender_kwarg)

    def consume(self, count=None, timeout=None):
        """Consume operations from the queue

        @param count: number of messages to consume before returning
        @param timeout: time in seconds to wait without receiving a message
        """
        self.rpc.consume(count, timeout)

    def cancel(self, block=True):
        """Cancel a call to consume() happening in another thread

        This could take up to RpcConnection.consumer_timeout to complete.

        @param block: if True, waits until the consumer has returned
        """
        self.rpc.cancel(block=block)


class RpcConnection(object):
    consumer_timeout = 1.0
    timeout_error = socket.timeout

    def __init__(self, name, uri, exchange, durable=False, auto_delete=False,
                 serializer=None, transport_options=None, ssl=False,
                 heartbeat=DEFAULT_HEARTBEAT, retry=None, errback=None, logger=None):
        """Set up a rpc connection

        @param name: name of destination service queue used by consumers
        @param uri: broker URI (e.g. 'amqp://guest:guest@localhost:5672//')
        @param exchange: name of exchange to create and use
        @param durable: if True, destination service queue and exchange will be
        created as durable
        @param auto_delete: if True, destination service queue and exchange
        will be deleted when all consumers are gone
        @param serializer: specify a serializer for message encoding
        @param transport_options: custom parameter dict for the transport backend
        @param heartbeat: amqp heartbeat interval
        @param retry: a RetryBackoff object, or None to use defaults
        @param errback: callback called within except block of connection failures
        """

        self.heartbeat_interval = heartbeat
        self.conn = Connection(uri, transport_options=transport_options,
                               ssl=ssl, heartbeat=self.heartbeat_interval)
        if heartbeat:
            # create a connection template for pooled connections. These cannot
            # have heartbeat enabled.
            self.pool_conn = Connection(
                uri, transport_options=transport_options,
                ssl=ssl)
        else:
            self.pool_conn = self.conn

        self._name = name
        self.exchange_name = "%s.%s" % ("gate", exchange)
        self.exchange = Exchange(name=self.exchange_name, type='direct',
                                 durable=durable, auto_delete=auto_delete)

        # visible attributes
        self.durable = durable
        self.auto_delete = auto_delete

        self.consumer = None

        self.linked_exceptions = {}

        self.serializer = serializer

        if retry is None:
            self.retry = RetryBackoff()
        else:
            self.retry = retry

        self.errback = errback

    @property
    def name(self):
        return self._name

    def fire(self, name, operation, args=None, **kwargs):
        """Send a message without waiting for a reply

        @param name: name of destination service queue
        @param operation: name of service operation to invoke
        @param args: dictionary of keyword args to pass to operation.
                     Use this OR kwargs.
        @param kwargs: additional args to pass to operation
        """

        if args:
            if kwargs:
                raise TypeError(
                    "specify args dict or keyword arguments, not both")
        else:
            args = kwargs

        d = dict(op=operation, args=args)
        headers = {'sender': '%s.rpc.%s' % (self.exchange_name, self._name)}

        dest = '%s.rpc.%s' % (self.exchange_name, self._name)

        def _fire(channel):
            with Producer(channel) as producer:
                producer.publish(d, routing_key=dest,
                                 headers=headers, serializer=self.serializer,
                                 exchange=self.exchange, declare=[self.exchange])

        if self.logger:
            self.logger.debug("sending message to %s", dest)
        with connections[self.pool_conn].acquire(block=True) as conn:
            _, channel = self.ensure(conn, _fire)
            conn.maybe_close_channel(channel)

    def call(self, name, operation, timeout=10, args=None, **kwargs):
        """Send a message and wait for reply

        @param name: name of destination service queue
        @param operation: name of service operation to invoke
        @param timeout: RPC timeout to await a reply
        @param args: dictionary of keyword args to pass to operation.
                     Use this OR kwargs.
        @param kwargs: additional args to pass to operation
        """

        if args:
            if kwargs:
                raise TypeError(
                    "specify args dict or keyword arguments, not both")
        else:
            args = kwargs

        # create a direct queue for the reply. This may end up being a
        # bottleneck for performance: each rpc call gets a brand new
        # exclusive queue. However this approach is used nova.rpc and
        # seems to have carried them pretty far. If/when this
        # becomes a bottleneck we can set up a long-lived backend queue and
        # use correlation_id to deal with concurrent RPC calls. See:
        #   http://www.rabbitmq.com/tutorials/tutorial-six-python.html
        msg_id = uuid.uuid4().hex

        # expire the reply queue shortly after the timeout. it will be
        # (lazily) deleted by the broker if we don't clean it up first
        queue_arguments = {'x-expires': int((timeout + 1) * 1000)}
        queue = Queue(name=msg_id, exchange=self.exchange, routing_key=msg_id,
                      durable=False, queue_arguments=queue_arguments)

        messages = []
        event = threading.Event()

        def _callback(body, message):
            messages.append(body)
            message.ack()
            event.set()

        d = dict(op=operation, args=args)
        headers = {'reply-to': msg_id, 'sender': '%s.rpc.%s' %
                   (self.exchange_name, self._name)}
        dest = '%s.rpc.%s' % (self.exchange_name, self._name)

        def _declare_and_send(channel):
            consumer = Consumer(channel, (queue,), callbacks=(_callback,))
            with Producer(channel) as producer:
                producer.publish(d, routing_key=dest, headers=headers,
                                 exchange=self.exchange, serializer=self.serializer)
            return consumer

        if self.logger:
            self.logger.debug("sending call to %s:%s", dest, operation)
        with connections[self.pool_conn].acquire(block=True) as conn:
            consumer, channel = self.ensure(conn, _declare_and_send)
            try:
                self._consume(
                    conn, consumer, timeout=timeout, until_event=event)

                # try to delete queue, but don't worry if it fails (will
                # expire)
                try:
                    queue = queue.bind(channel)
                    queue.delete(nowait=True)
                except Exception:
                    if self.logger:
                        self.logger.exception("error deleting queue")

            finally:
                conn.maybe_close_channel(channel)

        msg_body = messages[0]
        if msg_body.get('error'):
            raise_error(msg_body['error'])
        else:
            return msg_body.get('result')

    def _consume(self, connection, consumer, count=None, timeout=None, until_event=None):
        if count is not None:
            if count <= 0:
                raise ValueError("count must be >= 1")
            consumed = itertools.count(1)

        inner_timeout = self.consumer_timeout
        if timeout is not None:
            timeout = Countdown.from_value(timeout)
            inner_timeout = min(timeout.timeleft, inner_timeout)

        if until_event and until_event.is_set():
            return

        needs_heartbeat = connection.heartbeat and connection.supports_heartbeats
        if needs_heartbeat:
            time_between_tics = timedelta(seconds=connection.heartbeat / 2.0)
            if self.consumer_timeout > time_between_tics.seconds:
                msg = "consumer timeout (%s) must be half or smaller than the heartbeat interval %s" % (
                    self.consumer_timeout, connection.heartbeat)
                raise RpcError(msg)
            last_heartbeat_check = datetime.min

        reconnect = False
        declare = True
        while True:
            try:
                if declare:
                    consumer.consume()
                    declare = False

                if needs_heartbeat:
                    if datetime.now() - last_heartbeat_check > time_between_tics:
                        last_heartbeat_check = datetime.now()
                        connection.heartbeat_check()

                connection.drain_events(timeout=inner_timeout)
                if count and next(consumed) == count:
                    return

            except socket.timeout:
                pass
            except (connection.connection_errors, IOError):
                if self.logger:
                    self.logger.debug(
                        "Received error consuming", exc_info=True)
                self._call_errback()
                reconnect = True

            if until_event is not None and until_event.is_set():
                return

            if timeout:
                inner_timeout = min(inner_timeout, timeout.timeleft)
                if not inner_timeout:
                    raise self.timeout_error()

            if reconnect:
                self.connect(connection, (consumer,), timeout=timeout)
                reconnect = False
                declare = True

    def reply(self, connection, msg_id, body):
        def _reply(channel):
            with Producer(channel) as producer:
                producer.publish(
                    body, routing_key=msg_id, exchange=self.exchange,
                    serializer=self.serializer)

        if self.logger:
            self.logger.debug("replying to %s", msg_id)
        _, channel = self.ensure(connection, _reply)
        connection.maybe_close_channel(channel)

    def handle(self, operation, operation_name=None, sender_kwarg=None):
        """Handle an operation using the specified function

        @param operation: function to call for this operation
        @param operation_name: operation name. if unspecifed operation.__name__ is used
        @param sender_kwarg: optional keyword arg on operation to feed in sender name
        """
        if not self.consumer:
            self.consumer = RpcConsumer(self, self.conn,
                                        self._name, self.exchange)
        self.consumer.add_op(operation_name or operation.__name__, operation,
                             sender_kwarg=sender_kwarg)

    def consume(self, count=None, timeout=None):
        """Consume operations from the queue

        @param count: number of messages to consume before returning
        @param timeout: time in seconds to wait without receiving a message
        """
        self.consumer.consume(count, timeout)

    def cancel(self, block=True):
        """Cancel a call to consume() happening in another thread

        This could take up to RpcConnection.consumer_timeout to complete.

        @param block: if True, waits until the consumer has returned
        """
        if self.consumer:
            self.consumer.cancel(block=block)

    def disconnect(self):
        """Disconnects a consumer binding if exists
        """
        if self.consumer:
            self.consumer.disconnect()

    def link_exceptions(self, custom_exception=None, exception=None):
        """Link a custom exception thrown on the receiver to a exception
        """
        if custom_exception is None:
            raise ValueError("custom_exception must be set")
        if exception is None:
            raise ValueError("exception must be set")

        self.linked_exceptions[custom_exception] = exception

    def _call_errback(self):
        if not self.errback:
            return
        try:
            self.errback()
        except Exception:
            if self.logger:
                self.logger.exception("error calling errback..")

    def connect(self, connection, entities=None, timeout=None):
        if timeout is not None:
            timeout = Countdown.from_value(timeout)
        backoff = iter(self.retry)
        while True:

            this_backoff = next(backoff, False)

            try:
                channel = self._connect(connection)
                if entities:
                    for entity in entities:
                        entity.revive(channel)
                return channel

            except (connection.connection_errors, IOError):
                if this_backoff is False:
                    if self.logger:
                        self.logger.exception(
                            "Error connecting to broker. Giving up.")
                    raise
                self._call_errback()

            if timeout:
                timeleft = timeout.timeleft
                if not timeleft:
                    raise self.timeout_error()
                elif timeleft < this_backoff:
                    this_backoff = timeleft

            if self.logger:
                self.logger.exception(
                    "Error connecting to broker. Retrying in %ss", this_backoff)
            time.sleep(this_backoff)

    def _connect(self, connection):
        # close out previous connection first
        try:
            # dirty: breaking into kombu to force close the connection
            connection._close()
        except connection.connection_errors:
            pass

        connection.connect()
        return connection.channel()

    def ensure(self, connection, func, *args, **kwargs):
        """Perform an operation until success

        Repeats in the face of connection errors, persuant to retry policy
        """
        channel = None
        while True:
            try:
                if channel is None:
                    channel = connection.channel()
                return func(channel, *args, **kwargs), channel
            except (connection.connection_errors, IOError):
                self._call_errback()

            channel = self.connect(connection)

_OpSpec = namedtuple('_OpSpec', ['function', 'sender_kwarg'])


class RpcConsumer(object):

    def __init__(self, rpc, connection, name, exchange):
        self.rpc = rpc
        self.conn = connection
        self._name = name
        self.exchange = exchange

        self.ops = {}
        self.cancelled = threading.Event()
        self.consumer_lock = threading.Lock()

        self.queue_name = "%s.rpc.%s" % (self.rpc.exchange_name, self._name)
        self.queue_kwargs = dict(
            name=self.queue_name,
            exchange=self.exchange,
            routing_key=self.queue_name,
            durable=self.rpc.durable,
            auto_delete=self.rpc.auto_delete,
            queue_arguments={'x-expires': int(DEFAULT_QUEUE_EXPIRATION * 1000)})

        self.connect()

    def connect(self):
        self.rpc.ensure(self.conn, self._connect)

    def _connect(self, channel):
        self.queue = Queue(channel=channel, **self.queue_kwargs)
        self.queue.declare()

        self.consumer = Consumer(channel, [self.queue],
                                 callbacks=[self._callback])
        self.consumer.consume()

    def disconnect(self):
        self.consumer.cancel()
        self.conn.release()

    def consume(self, count=None, timeout=None):

        # hold a lock for the duration of the consuming. this prevents
        # multiple consumers and allows cancel to detect when consuming
        # has ended.
        if not self.consumer_lock.acquire(False):
            raise Exception("only one consumer thread may run concurrently")

        try:
            self.rpc._consume(self.conn, self.consumer, count=count,
                             timeout=timeout, until_event=self.cancelled)
        finally:
            self.consumer_lock.release()
            self.cancelled.clear()

    def cancel(self, block=True):
        self.cancelled.set()
        if block:
            # acquire the lock and release it immediately
            with self.consumer_lock:
                pass

    def _callback(self, body, message):
        reply_to = None
        ret = None
        err = None
        err_dict = None
        try:
            reply_to = message.headers.get('reply-to')

            try:
                op = str(body['op'])
                args = body.get('args')
            except Exception as e:
                if self.logger:
                    self.logger.warn(
                        "Failed to interpret message body: %s", body,
                        exc_info=True)
                raise BadRequestError("Invalid request: %s" % str(e))

            op_spec = self.ops.get(op)
            if not op_spec:
                raise UnknownOperationError("Unknown operation: " + op)
            op_fun = op_spec.function

            # stick the sender into kwargs if handler requested it
            if op_spec.sender_kwarg:
                sender = message.headers.get('sender')
                args[op_spec.sender_kwarg] = sender

            try:
                ret = op_fun(**args)
            except TypeError as e:
                if self.logger:
                    self.logger.exception(
                        "Type error with handler for %s:%s", self._name, op)
                raise BadRequestError("Type error: %s" % str(e))
            except Exception:
                raise

        except Exception:
            err = sys.exc_info()
        finally:
            if err:
                err_dict, is_known_error = self._wrap_error(err)
                if is_known_error:
                    exc_type = err_dict['exc_type']
                    if exc_type and exc_type.startswith(ERROR_PREFIX):
                        exc_type = exc_type[len(ERROR_PREFIX):]
                    if self.logger:
                        self.logger.info(
                            "Known '%s' error in handler %s:%s: %s", exc_type,
                            self._name, op, err_dict['value'])
                else:
                    if self.logger:
                        self.logger.error(
                            "Unknown '%s' error in handler %s:%s: %s",
                            err_dict['exc_type'], self._name, op,
                            err_dict['value'], exc_info=err)

            if reply_to:
                reply = dict(result=ret, error=err_dict)
                self.rpc.reply(self.conn, reply_to, reply)

            message.ack()

    def _wrap_error(self, exc_info):
        tb = "".join(traceback.format_exception(*exc_info))

        # some error types are specific to dashi (not underlying
        # service code). These get raised with the same type on
        # the client side. Identify them by prefixing the package
        # name on the exc_type.

        exc_type = exc_info[0]

        # Check if there is a dashi exception linked to this custom exception
        linked_exception = self.dashi.linked_exceptions.get(exc_type)
        if linked_exception:
            exc_type = linked_exception

        known_type = ERROR_TYPE_MAP.get(exc_type.__name__)
        is_known = known_type and exc_type is known_type
        if is_known:
            exc_type_name = ERROR_PREFIX + exc_type.__name__
        else:
            exc_type_name = exc_type.__name__

        return dict(exc_type=exc_type_name, value=str(exc_info[1]),
                    traceback=tb), is_known

    def add_op(self, name, fun, sender_kwarg=None):
        if not callable(fun):
            raise ValueError("operation function must be callable")

        self.ops[name] = _OpSpec(fun, sender_kwarg)


class Countdown(object):
    _time_func = time.time

    def __init__(self, timeout, time_func=None):
        if time_func is not None:
            self.time_func = time_func
        self.timeout = timeout
        self.expires = self.time_func() + timeout

    @classmethod
    def from_value(cls, timeout):
        """Wraps a timeout value in a Countdown, unless it already is
        """
        if isinstance(timeout, cls):
            return timeout
        return cls(timeout)

    @property
    def expired(self):
        return self.time_func() >= self.expires

    @property
    def timeleft(self):
        """Number of seconds remaining before timeout
        """
        return max(0.0, self.expires - self.time_func())

    @property
    def delta_seconds(self):
        """difference in seconds. can be negative"""
        return self.expires - self.time_func()


class RetryBackoff(object):

    def __init__(self, max_attempts=0, backoff_start=0.5, backoff_step=0.5, backoff_max=30, timeout=None):
        self.max_attempts = int(max_attempts)
        self.backoff_start = float(backoff_start)
        self.backoff_step = float(backoff_step)
        self.backoff_max = float(backoff_max)

        self.timeout = Countdown.assure(timeout) if timeout else None

    def __iter__(self):
        retry = 1
        backoff = self.backoff_start

        while not self.max_attempts or retry <= self.max_attempts:

            if self.timeout:
                timeleft = self.timeout.timeleft
                if not timeleft:
                    return
                backoff = max(backoff, timeleft)

            yield backoff

            backoff = min(backoff + self.backoff_step, self.backoff_max)
            retry += 1


def raise_error(error):
    """Intakes a dict of remote error information and raises a RpcError
    """
    exc_type = error.get('exc_type')
    if exc_type and exc_type.startswith("gate.common.exceptions."):
        exc_type = exc_type[len("gate.common.exceptions."):]
        exc_cls = ERROR_TYPE_MAP.get(exc_type, RpcError)
    else:
        exc_cls = RpcError
    raise exc_cls(**error)

ERROR_TYPES = (BadRequestError, NotFoundError,
               UnknownOperationError, WriteConflictError)
ERROR_TYPE_MAP = dict((cls.__name__, cls) for cls in ERROR_TYPES)
