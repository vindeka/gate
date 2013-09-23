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

import uuid
import unittest
import threading

from oslo.config import cfg

from gate.engine.server import EngineServer
from gate.engine.client import EngineClient
from gate.engine.common.storage.drivers.memory import MemoryDriver
from test.unit.gate import BaseTestCase


class ProcessTest(BaseTestCase):

    def __init__(self, *args):
        cfg.CONF(args=[], project='gate', prog='engine-server')
        self.enableFakeTransport()
        self.setupLogging()
        super(ProcessTest, self).__init__(*args)

    def _start_server(self, storage_driver=None, topic=None, host=None, allow_stop=True):
        if not storage_driver:
            storage_driver = MemoryDriver('memory:///')

        server = EngineServer(
            host or 'test.host', storage_driver=storage_driver, topic=topic, allow_stop=allow_stop)

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

    def assertValidId(self, result):
        self.assertTrue(result is not None)
        self.assertTrue('uuid' in result)
        self.assertTrue(result['uuid'] is not None)

    def test_default_topic(self):
        thread = self._start_server()
        client = EngineClient(thread.server._transport)

        self.assertEqual(thread.server.topic, client.topic)
        self._stop_server(client, thread)

    def test_custom_topic(self):
        thread = self._start_server(topic='custom')
        client = EngineClient(thread.server._transport, topic='custom')

        self.assertEqual(thread.server.topic, client.topic)
        self._stop_server(client, thread)

    def test_case_create(self):
        thread = self._start_server()
        client = EngineClient(thread.server._transport)

        account_uuid = str(uuid.uuid4())
        result = client.case_create(account_uuid=account_uuid, name='testname')
        
        self.assertValidId(result)

        result2 = client.case_get(result['uuid'])
        self._stop_server(client, thread)

        self.assertEquals(result2['uuid'], result['uuid'])
        self.assertEquals(result2['name'], 'testname')

    def test_case_update(self):
        thread = self._start_server()
        client = EngineClient(thread.server._transport)

        account_uuid = str(uuid.uuid4())
        result = client.case_create(account_uuid=account_uuid, name='testname')
        
        self.assertValidId(result)

        result2 = client.case_update(result['uuid'], name='newname')
        self._stop_server(client, thread)

        self.assertEquals(result2['uuid'], result['uuid'])
        self.assertTrue('area' not in result2)
        self.assertEquals(result2['name'], 'newname')

    def test_case_delete(self):
        thread = self._start_server()
        client = EngineClient(thread.server._transport)

        account_uuid = str(uuid.uuid4())
        result = client.case_create(account_uuid=account_uuid, name='testname')
        
        self.assertValidId(result)

        result2 = client.case_delete(result['uuid'])
        result3 = client.case_get(result['uuid'])
        self._stop_server(client, thread)

        self.assertTrue(result2)
        self.assertTrue(result3 is None)

    def test_case_list(self):
        thread = self._start_server()
        client = EngineClient(thread.server._transport)

        account_uuid = str(uuid.uuid4())
        one = client.case_create(account_uuid=account_uuid, name='one')
        two = client.case_create(account_uuid=account_uuid, name='two')
        
        self.assertValidId(one)
        self.assertValidId(two)

        items = client.case_list(account_uuid)
        self._stop_server(client, thread)

        found = 0
        for item in items:
            if item['uuid'] == one['uuid']:
                found += 1
                self.assertEquals(item['name'], 'one')
            elif item['uuid'] == two['uuid']:
                found += 1
                self.assertEquals(item['name'], 'two')

        self.assertEquals(found, 2)

if __name__ == '__main__':
    unittest.main()
