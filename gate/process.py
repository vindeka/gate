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

from gate.common.daemon import Daemon
from gate.common.utils import get_logger, config_true_value, readconf
from gate.common.pipeline import Pipelines
from gate.common.objs import MemoryDataObject
from gate.common.swift import init_swift

class Process(object):
    """
    Process object that is passed through the pipeline so information about the
    processing processs can be published.
    """

    def __init__(self, conn, exchange, pipeline, body, object_inline_size = 15728640, compression = False):
        self.connection = conn
        self.exchange = exchange
        self.pipeline = pipeline
        self.obj = body
        self.object_inline_size = object_inline_size
        self.compression = compression

    def process(self):
        """ Process the object through the timeline and finalizes to the engine. """
        exc = None
        try:
            self.pipeline.process(self, self.obj)
        except Exception, e:
            self.set_exception(e)
            exc = e
        comm = None
        if self.compression:
            comm = 'snappy'
        producer = kombu.Producer(self.connection, self.exchange, routing_key='engine',
            serializer='pickle', compression = comm)
        headers = {
            'pipeline': self.pipeline.name,
        }
        producer.publish(self.obj, 'engine', headers = headers, declare = ['engine'],
            retry = True)
        self.finalize()
        if exc:
            raise ecx

    def publish_obj(self, case_id, type, name, path, data = None, attrs = None):
        """ Published a newly identified object. """
        if not case_id or not type or not name or not path:
            raise Exception('Publish object call failed missing required argument.')
        obj = None
        if not data or len(data) <= 0:
            obj = MemoryDataObject(case_id, type, name, path, data, attrs)
        elif len(data) <= self.object_inline_size:
            obj = MemoryDataObject(case_id, type, name, path, data, attrs)
        else:
            """TODO: Handle the URLDataObject"""
            pass

    def set_exception(self, exc):
        """ Set exception information on object. """
        if not self.obj:
            return
        if not self.obj.exceptions:
            self.obj.exceptions = list()
        self.obj.exceptions.append(exc)

class ProcessServer(Daemon):
    """Process objects as the appear in the queue."""

    def __init__(self, conf):
        self.conf = conf
        self.logger = get_logger(conf, log_route='process-server', log_to_console = True)
        self.gate_dir = conf.get('gate_dir', '/etc/gate')
        self.swift_conf = readconf(conf.get('__file__'), 'object-store')
        broker_conf = readconf(conf.get('__file__'), 'broker')
        self.broker_url = broker_conf.get('broker_url', 'amqp://guest:guest@localhost:5672//')
        self.use_ssl = config_true_value(broker_conf.get('use_ssl', 'False'))
        self.pool_limit = int(broker_conf.get('pool_limit', '10'))
        self.connection_timeout = int(broker_conf.get('connection_timeout', '5'))
        self.failover_strategy = broker_conf.get('failover_strategy', 'round-robin')
        self.prefetch_count = int(broker_conf.get('prefetch_count', '1'))
        self.object_inline_size = int(broker_conf.get('object_inline_size', '15728640'))
        self.compression = config_true_value(broker_conf.get('compression', 'None'))
        
    def load(self):
        """Loads the pipelines."""
        self.pipelines = Pipelines(self.gate_dir)
        self.logger.info('%d pipelines initilized.' % len(self.pipelines))

    def connect(self):
        """Connect to the message broker and object store."""
        conn = kombu.Connection(self.broker_url, ssl=self.use_ssl, connect_timeout=self.connection_timeout,
            failover_strategy=self.failover_strategy)
        self.pool = conn.Pool(self.pool_limit)
        with self.pool.acquire(block = True) as c:
            self.logger.info('Connection to message broker established: %r' % c.as_uri())
        self.exchange = kombu.Exchange('gate', 'direct', durable=True)
        self.queue = kombu.Queue('process', exchange=self.exchange, routing_key='process')
        swift_enable = config_true_value(self.swift_conf.get('enabled', 'False'))
        if swift_enable:
            try:
                init_swift(self.swift_conf)
            except Exception, e:
                self.logger.error('%s' % e)
                self.close()
                return False
        return True

    def close(self):
        """Close all connections."""
        self.pool.force_close_all()
        self.logger.info('Connection to message broker closed.')

    def run_forever(self, *args, **kwargs):
        """Run the process continuously."""
        self.load()
        if not self.connect():
            return
        while True:
            with self.pool.acquire() as conn:
                with kombu.Consumer(conn, self.queue, callbacks=[self.message]) as cons:
                    cons.qos(prefetch_count = self.prefetch_count)
                    self.connection = conn

                    try:
                        recv = kombu.eventloop(conn, timeout = self.connection_timeout, limit = 1)
                        while True:
                            recv.next()
                    except socket.timeout:
                        self.logger.info('Timeout occurred with connection to broker.')
        self.connection = None
        self.close()


    def run_once(self, *args, **kwargs):
        """Run the process one pass."""
        self.load()
        self.connect()
        with self.pool.acquire() as conn:
            with kombu.Consumer(conn, self.queue, callbacks=[self.message]) as cons:
                cons.qos(prefetch_count = self.prefetch_count)

                try:
                    recv = kombu.eventloop(conn, timeout = self.connection_timeout, limit = 1)
                except socket.timeout:
                    self.logger.info('Timeout occurred with connection to broker.')
        self.connection = None
        self.close()

    def message(body, msg):
        """Handles the message from the queue."""
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
        process = Process(self.connection, self.exchange, body,
            self.object_inline_size, self.compression)
        try:
            process.process()
        except Exception, e:
            if process.obj and process.obj.id:
                self.logger.debug('Error processing object(%r) with exception: %r', process.obj.id, e)
            else:
                self.logger.debug('Error processing with exception: %r', e)
        msg.ack()
