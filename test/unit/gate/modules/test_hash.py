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

import unittest
import threading
import hashlib

from oslo.config import cfg

from gate.common import log as logging
from gate.process import ProcessServer
from gate.process.client import ProcessClient
from gate.process.modules.hash import HashModule
from test import FakePipeline, FakePipelineDriver


TEST_DATA = "raw test data"


class HashModuleTest(unittest.TestCase):

    def __init__(self, *args):
        cfg.CONF(args=[], project='gate', prog='process-server')
        cfg.CONF.transport_driver = 'fake'
        cfg.CONF.transport_url = 'fake:'
        logging.setup('gate')
        super(HashModuleTest, self).__init__(*args)

    def _start_server(self, pipelines=dict(), topic=None, host=None, allow_stop=True):
        server = ProcessServer(host or 'test.host', pipelines, topic=topic, allow_stop=allow_stop)

        thread = threading.Thread(target=server.start)
        thread.daemon = True
        thread.server = server
        thread.start()

        return thread

    def _stop_server(self, client, thread, topic=None):
        client = client._client
        if topic is not None:
            client = client.prepare(topic=topic)
        client.cast({}, 'stop')
        thread.join(timeout=30)

    def test_default(self):
        pipelines = dict()
        pipe = FakePipeline('hashing')
        pipe.add_module(HashModule(dict()))
        pipelines['hashing'] = pipe

        thread = self._start_server(pipelines=pipelines)
        client = ProcessClient(thread.server._transport)

        data = client.process_url('hashing', dict(), 'fake://blank')

        self._stop_server(client, thread)

        self.assertTrue('hash-md5' in data)
        self.assertTrue('hash-sha1' in data)
        self.assertTrue('hash-sha256' in data)
        self.assertTrue('hash-sha512' in data)

    def test_prefix(self):
        pipelines = dict()
        pipe = FakePipeline('hashing')
        pipe.add_module(HashModule({
            'prefix': 'verify'
        }))
        pipelines['hashing'] = pipe

        thread = self._start_server(pipelines=pipelines)
        client = ProcessClient(thread.server._transport)

        data = client.process_url('hashing', dict(), 'fake://blank')

        self._stop_server(client, thread)

        self.assertTrue('verify-hash-md5' in data)
        self.assertTrue('verify-hash-sha1' in data)
        self.assertTrue('verify-hash-sha256' in data)
        self.assertTrue('verify-hash-sha512' in data)

    def test_sel_md5(self):
        pipelines = dict()
        pipe = FakePipeline('hashing')
        pipe.add_module(HashModule({
            'algorithms': 'md5',
        }))
        pipelines['hashing'] = pipe

        thread = self._start_server(pipelines=pipelines)
        client = ProcessClient(thread.server._transport)

        data = client.process_url('hashing', dict(), 'fake://blank')

        self._stop_server(client, thread)

        self.assertTrue('hash-md5' in data)
        self.assertFalse('hash-sha1' in data)
        self.assertFalse('hash-sha256' in data)
        self.assertFalse('hash-sha512' in data)

    def test_sel_sha1(self):
        pipelines = dict()
        pipe = FakePipeline('hashing')
        pipe.add_module(HashModule({
            'algorithms': 'sha1',
        }))
        pipelines['hashing'] = pipe

        thread = self._start_server(pipelines=pipelines)
        client = ProcessClient(thread.server._transport)

        data = client.process_url('hashing', dict(), 'fake://blank')

        self._stop_server(client, thread)

        self.assertTrue('hash-sha1' in data)
        self.assertFalse('hash-md5' in data)
        self.assertFalse('hash-sha256' in data)
        self.assertFalse('hash-sha512' in data)

    def test_sel_sha256(self):
        pipelines = dict()
        pipe = FakePipeline('hashing')
        pipe.add_module(HashModule({
            'algorithms': 'sha256',
        }))
        pipelines['hashing'] = pipe

        thread = self._start_server(pipelines=pipelines)
        client = ProcessClient(thread.server._transport)

        data = client.process_url('hashing', dict(), 'fake://blank')

        self._stop_server(client, thread)

        self.assertTrue('hash-sha256' in data)
        self.assertFalse('hash-sha1' in data)
        self.assertFalse('hash-md5' in data)
        self.assertFalse('hash-sha512' in data)

    def test_sel_sha512(self):
        pipelines = dict()
        pipe = FakePipeline('hashing')
        pipe.add_module(HashModule({
            'algorithms': 'sha512',
        }))
        pipelines['hashing'] = pipe

        thread = self._start_server(pipelines=pipelines)
        client = ProcessClient(thread.server._transport)

        data = client.process_url('hashing', dict(), 'fake://blank')

        self._stop_server(client, thread)

        self.assertTrue('hash-sha512' in data)
        self.assertFalse('hash-sha1' in data)
        self.assertFalse('hash-sha256' in data)
        self.assertFalse('hash-md5' in data)

    def test_result(self):
        pipelines = dict()
        pipe = FakePipeline('hashing')
        pipe.add_module(HashModule(dict()))
        pipelines['hashing'] = pipe

        thread = self._start_server(pipelines=pipelines)
        client = ProcessClient(thread.server._transport)

        data = client.process_raw('hashing', dict(), TEST_DATA)

        self._stop_server(client, thread)

        self.assertTrue('hash-md5' in data)
        md5 = hashlib.md5()
        md5.update(TEST_DATA)
        self.assertEqual(md5.hexdigest(), data['hash-md5'])

        self.assertTrue('hash-sha1' in data)
        sha1 = hashlib.sha1()
        sha1.update(TEST_DATA)
        self.assertEqual(sha1.hexdigest(), data['hash-sha1'])

        self.assertTrue('hash-sha256' in data)
        sha256 = hashlib.sha256()
        sha256.update(TEST_DATA)
        self.assertEqual(sha256.hexdigest(), data['hash-sha256'])

        self.assertTrue('hash-sha512' in data)
        sha512 = hashlib.sha512()
        sha512.update(TEST_DATA)
        self.assertEqual(sha512.hexdigest(), data['hash-sha512'])
