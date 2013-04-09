#!/usr/bin/python
# -*- coding: utf-8 -*-
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

from gate.process import ProcessServer
from gate.common.utils import readconf
from gate.common.objs import MemoryDataObject
from gate.common.pipeline import Pipeline
from test.unit import FakeLogger

class TestModule(object):

    def __init__(self, func):
        self.func = func

    def __call__(self):
        pass

    def process(self, proc, data_obj):
        self.func(proc, data_obj)

class TestPipeline(Pipeline):

    def __init__(self, name, func):
        self.name = name
        self.objects = []
        self.inited = False
        self.objects.append(TestModule(func))

class ProcessTest(unittest.TestCase):

    def setUp(self):
        self.conf_file = readconf('test/etc/gate.conf', 'process-server')
        self.process = ProcessServer(self.conf_file)
        self.process.logger = FakeLogger('process-server')
        with open('test/data/opensource.svg') as fp:
            self.object_data = fp.read()

    def test_broker_memory_object(self):
        self.data_obj = None
        obj_id = uuid.uuid4()
        case_id = uuid.uuid4()
        parent_id = uuid.uuid4()
        data = self.object_data[:]
        def test_func(proc, data_obj):
            self.data_obj = data_obj

        pipelines = dict()
        pipelines['testing'] = TestPipeline('testing', test_func)
        self.assertTrue(self.process.load(force=True, pipelines=pipelines))
        self.process.transport.logger = FakeLogger('transport:memcached')
        self.assertTrue(self.process.connect(force=True))

        mem_obj = MemoryDataObject(
            obj_id,
            None,
            self.object_data,
            case_id=case_id,
            name='opensource.svg',
            path='/opensource.svg',
            parent_id=parent_id,
            type='file',
            )
        conn = self.process.connection
        with conn.Producer(serializer='pickle',
                           compression=None) as producer:
            producer.publish(mem_obj, exchange=self.process.exchange,
                routing_key='gate.process', declare=[self.process.queue],
                headers={'pipeline':'testing'})
        self.process.run_once()
        self.process.close()

        self.assertTrue(self.data_obj)
        self.assertEqual(obj_id, self.data_obj.id)
        self.assertEqual(case_id, self.data_obj.get('case_id'))
        self.assertEqual('opensource.svg', self.data_obj.get('name'))
        self.assertEqual('/opensource.svg', self.data_obj.get('path'))
        self.assertEqual(parent_id, self.data_obj.get('parent_id'))
        self.assertEqual('file', self.data_obj.get('type'))
        self.assertEqual(data, self.data_obj.read())

if __name__ == '__main__':
    unittest.main()
