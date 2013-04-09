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

import os
import sys
import kombu
import socket
import uuid

from kombu.mixins import ConsumerMixin
from gate.common.daemon import Daemon
from gate.common.utils import get_logger, config_true_value, readconf, \
    load_egg
from gate.common.pipeline import load_pipelines
from gate.common.objs import MemoryDataObject, FileDataObject, URLDataObject


class Process(object):

    """
    Process object that is passed through the pipeline so information about the
    processing processs can be published.
    """

    def __init__(self, server, pipeline, body):
        self.server = server
        self.compression = server.compression
        self.connection = server.connection
        self.pipeline = pipeline
        self.obj = body

    def _publish(self, obj, type, headers = None):
        comm = None
        if isinstance(obj, MemoryDataObject) and self.compression:
            comm = 'snappy'
        head = {
            'type': type
            }
        if headers:
            head.update(headers)
        q = kombu.Queue('gate.engine', self.server.exchange, 'gate.engine')
        with self.connection.Producer(serializer='pickle',compression=comm) as producer:
            producer.publish(obj, exchange=self.server.exchange,
                routing_key='gate.engine', declare=[q],
                headers=head)

    def _publish_process(self, obj, pipeline, headers = None):
        comm = None
        if isinstance(obj, MemoryDataObject) and self.compression:
            comm = 'snappy'
        head = {
            'pipeline': pipeline
            }
        if headers:
            head.update(headers)
        with self.connection.Producer(serializer='pickle',compression=comm) as producer:
            producer.publish(obj, exchange=self.server.exchange,
                routing_key='gate.engine', declare=[self.server.queue],
                headers=head)

    def _finalize(self):
        self.obj.pack()
        headers = {
            'pipeline': self.pipeline.name,
            }
        self._publish(self.obj, 'finalize', headers = headers)

    def process(self):
        """ Process the object through the timeline and finalizes to the engine. """

        self.obj.unpack(self)
        exc = None
        try:
            self.pipeline.process(self, self.obj)
        except Exception, e:
            self.set_exception(e)
            exc = e
        self._finalize()
        if exc:
            raise exc

    def publish_obj(self, obj):
        """ Published a newly identified object. """

        if not obj.id:
            obj.id = uuid.uuid4()

        obj.reset()
        if isinstance(obj, FileDataObject):
            size = obj.size()
            if size <= self.server.object_inline_size:
                obj = MemoryDataObject(obj.id, obj.app, obj.read(), 
                                       obj.items())
            else:
                case_id = obj.get('case_id', uuid.uuid4())
                path = obj.get('path', uuid4.uuid4())
                key  = '%s/%s' % (case_id, path)
                if not self.server.transport.put_stream(key, obj):
                    raise Exception('Error placing object on transport: %s' % key)
                key = '%s:%s' % (self.server.transport.name, key)
                obj = URLDataObject(obj.id, obj.app, key, obj.items())
        obj.pack()
        self._publish_process(obj, self.pipeline.name)
        
    def set_exception(self, exc):
        """ Set exception information on object. """

        if not self.obj:
            return
        if not self.obj.exceptions:
            self.obj.exceptions = list()
        self.obj.exceptions.append(exc)

class ProcessConsumer(ConsumerMixin):

    def __init__(self, server):
        self.server = server
        self.logger = server.logger
        self.connection = server.connection
        self.pipelines = server.pipelines
        self.queue = server.queue

    def on_consume_ready(self, connection, channel, consumers, **kwargs):
        self.logger.debug('Process consumer is ready to consume.')

    def get_consumers(self, Consumer, channel):
        return [Consumer(queues=[self.queue],
                         callbacks=[self.on_process])]

    def on_process(self, body, msg):
        try:
            p_name = msg.headers['pipeline']
        except:
            self.logger.debug('Invalid message header, missing pipeline.')
            return
        try:
            pipeline = self.pipelines[p_name]
        except:
            self.logger.debug('Missing pipeline: %s' % p_name)
            return
        self.logger.debug('Recieved message to process on pipeline:%s.' % p_name)
        process = Process(self.server, pipeline, body)
        try:
            process.process()
        except Exception, e:
            raise
            if process.obj and process.obj.id:
                self.logger.debug('Error processing object(%r) with exception: %r'
                                  , process.obj.id, e)
            else:
                self.logger.debug('Error processing with exception: %r'
                                  , e)
        msg.ack()

class ProcessServer(Daemon):

    """Process objects as the appear in the queue."""

    def __init__(self, conf):
        self.conf = conf
        self.logger = get_logger(conf, log_route='process-server',
            log_to_console = True)
        self.gate_dir = conf.get('gate_dir', '/etc/gate')
        broker_conf = readconf(conf.get('__file__'), 'broker')
        self.broker_url = broker_conf.get('broker_url',
                'amqp://guest:guest@localhost:5672//')
        self.use_ssl = config_true_value(broker_conf.get('use_ssl',
                'False'))
        self.connection_timeout = \
            int(broker_conf.get('connection_timeout', '5'))
        self.failover_strategy = broker_conf.get('failover_strategy',
                'round-robin')
        self.prefetch_count = int(broker_conf.get('prefetch_count', '1'
                                  ))
        self.object_inline_size = \
            int(broker_conf.get('object_inline_size', '15728640'))
        self.compression = \
            config_true_value(broker_conf.get('compression', 'None'))
        self.exchange = kombu.Exchange('gate', 'direct', durable=True)
        self.queue = kombu.Queue('gate.process', self.exchange,
            'gate.process')
        self.loaded = False
        self.connected = False

    def load(self, force=False, pipelines=None):
        """Loads the transports and pipelines."""

        if self.loaded and not force:
            return True
        self.connected = False
        transport = self.conf.get('large_object_transport', None)
        if not transport:
            self.logger.error('Missing large_object_transport config value.'
                              )
            return False
        trans_conf = readconf(self.conf.get('__file__'), 'transport:%s'
                              % transport)
        try:
            use = trans_conf.get('use')
            trans = load_egg(use, 'gate.transport_factory')
            self.transport = trans(trans_conf)
        except Exception, e:
            self.logger.error('Failed to load transport:%s %r'
                              % (transport, e))
            return False
        if not pipelines:
            self.pipelines = load_pipelines(self.gate_dir)
        else:
            self.pipelines = pipelines
        self.logger.info('%d pipelines initilized.'
                         % len(self.pipelines))
        self.loaded = True
        return True

    def connect(self, force=False):
        """Connect to the message broker and object store."""

        if self.connected and not force:
            return True
        if not self.transport.open():
            self.logger.error('Transport connection failed.')
            return False

        self.connection = self.get_connection()
        self.connection.connect()
        self.connected = True
        return True

    def get_connection(self):
        return kombu.Connection(self.broker_url,
            ssl=self.use_ssl, connect_timeout=self.connection_timeout,
            failover_strategy=self.failover_strategy)

    def close(self):
        if self.connected:
            self.transport.close()
            self.connection.close()

    def run_forever(self, *args, **kwargs):
        """Run the process continuously."""

        if not self.load() or not self.connect():
            return
        try:
            ProcessConsumer(self).run()
        except:
            self.close()
            raise
        self.close()


    def run_once(self, *args, **kwargs):
        """Run the process one pass."""

        if not self.load() or not self.connect():
            return
        try:
            c = ProcessConsumer(self)
            if c.restart_limit.can_consume(1):
                for _ in c.consume(limit=1):
                    pass
        except:
            self.close()
            raise
        self.close()

