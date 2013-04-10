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

import sys
import uuid
import unittest
import kombu
import logging
import hashlib

from gate.process import ProcessServer
from gate.common.utils import readconf
from gate.common.objs import MemoryDataObject
from gate.modules.hash import HashModule
from test import FakeLogger, FakePipeline, FakeModule

class HashModuleTest(unittest.TestCase):

    def setUp(self):
        self.conf_file = readconf('test/etc/gate.conf', 'process-server')
        self.process = ProcessServer(self.conf_file)
        self.process.logger = FakeLogger()
        with open('test/data/opensource.svg') as fp:
            self.object_data = fp.read()
        self.md5 = hashlib.md5(self.object_data).hexdigest()
        self.sha1 = hashlib.sha1(self.object_data).hexdigest()
        self.sha256 = hashlib.sha256(self.object_data).hexdigest()
        self.sha512 = hashlib.sha512(self.object_data).hexdigest()

    def test_module(self, compress=None):
        def end_module(proc, data_obj):
            self.assertTrue(data_obj.has_key('hash-md5'))
            self.assertTrue(data_obj.has_key('hash-sha1'))
            self.assertTrue(data_obj.has_key('hash-sha256'))
            self.assertTrue(data_obj.has_key('hash-sha512'))
            self.data_md5 = data_obj.get('hash-md5')
            self.data_sha1 = data_obj.get('hash-sha1')
            self.data_sha256 = data_obj.get('hash-sha256')
            self.data_sha512 = data_obj.get('hash-sha512')

        hash = HashModule(self.conf_file)
        hash.logger = FakeLogger()
        pipelines = dict()
        pipe = FakePipeline('hashing')
        pipe.add_module(hash)
        pipe.add_module(FakeModule(end_module))
        pipelines['hashing'] = pipe

        self.assertTrue(self.process.load(force=True, pipelines=pipelines))
        self.process.transport.logger = FakeLogger()
        self.assertTrue(self.process.connect(force=True))

        mem_obj = MemoryDataObject(
            uuid.uuid4(),
            None,
            self.object_data,
            case_id=uuid.uuid4(),
            name='opensource.svg',
            path='/opensource.svg',
            parent_id=uuid.uuid4(),
            type='file',
            )
        conn = self.process.connection
        with conn.Producer(serializer='pickle',
                           compression=compress) as producer:
            producer.publish(mem_obj, exchange=self.process.exchange,
                routing_key='gate.process', declare=[self.process.queue],
                headers={'pipeline':'hashing'})
        self.process.run_once()
        self.process.close()

        self.assertEqual(self.md5, self.data_md5)
        self.assertEqual(self.sha1, self.data_sha1)
        self.assertEqual(self.sha256, self.data_sha256)
        self.assertEqual(self.sha512, self.data_sha512)
