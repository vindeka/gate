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

from oslo.config import cfg

from gate.common import log as logging
from gate.process.server import ProcessServer
from gate.process.client import ProcessClient
from test import FakePipeline, FakePipelineDriver


class ProcessTest(unittest.TestCase):

    def __init__(self, *args):
        cfg.CONF(args=[], project='gate', prog='process-server')
        cfg.CONF.transport_driver = 'fake'
        cfg.CONF.transport_url = 'fake:'
        logging.setup('gate')
        super(ProcessTest, self).__init__(*args)

    def _start_server(self, pipelines=dict(), topic=None, host=None, allow_stop=True):
        pipeline_driver = FakePipelineDriver()
        for key, value in pipelines.items():
            pipeline_driver.add_pipeline(value, name=key)

        server = ProcessServer(
            host or 'test.host', pipeline_driver=pipeline_driver, topic=topic, allow_stop=allow_stop)

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

    def test_default_topic(self):
        thread = self._start_server()
        client = ProcessClient(thread.server._transport)

        self.assertEqual(thread.server.topic, client.topic)
        self._stop_server(client, thread)

    def test_custom_topic(self):
        thread = self._start_server(topic='custom')
        client = ProcessClient(thread.server._transport, topic='custom')

        self.assertEqual(thread.server.topic, client.topic)
        self._stop_server(client, thread)

    def test_pipeline_process(self):
        def empty_func(bundle):
            bundle.data['called'] = True
            return bundle

        pipelines = dict()
        pipelines['empty'] = FakePipeline('empty', empty_func)
        thread = self._start_server(pipelines=pipelines)
        client = ProcessClient(thread.server._transport)

        data = client.process_url('empty', dict(), 'fake://blank')

        self._stop_server(client, thread)

        self.assertTrue('called' in data)
        self.assertTrue(data['called'])

    def test_pipeline_exceptions(self):
        def empty_func(bundle):
            raise Exception("test")

        pipelines = dict()
        pipelines['empty'] = FakePipeline('empty', empty_func)
        thread = self._start_server(pipelines=pipelines)
        client = ProcessClient(thread.server._transport)

        data = client.process_url('empty', dict(), 'fake://blank')

        self._stop_server(client, thread)

        self.assertTrue('exceptions' in data)
        self.assertEqual(data['exceptions'][0]['name'], "Exception")
        self.assertEqual(data['exceptions'][0]['msg'], "test")

    def test_pipeline_multi_modules(self):
        def test_func(bundle):
            bundle.data['func'] = True
            return bundle

        def test_func2(bundle):
            bundle.data['func2'] = True
            return bundle

        pipelines = dict()
        pipe = FakePipeline('multi')
        pipe.add_func(test_func)
        pipe.add_func(test_func2)
        pipelines['multi'] = pipe
        thread = self._start_server(pipelines=pipelines)
        client = ProcessClient(thread.server._transport)

        data = client.process_url('multi', dict(), 'fake://blank')

        self._stop_server(client, thread)

        self.assertFalse('exceptions' in data)
        self.assertTrue('func' in data)
        self.assertTrue('func2' in data)
        self.assertTrue(data['func'])
        self.assertTrue(data['func'])

if __name__ == '__main__':
    unittest.main()
