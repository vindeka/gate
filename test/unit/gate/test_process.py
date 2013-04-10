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
        self.process.logger = FakeLogger()
        with open('test/data/opensource.svg') as fp:
            self.object_data = fp.read()

    def _broker_mem_obj(self, compress=None):
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
        self.process.transport.logger = FakeLogger()
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
                           compression=compress) as producer:
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

    def test_broker_mem_obj(self):
        self._broker_mem_obj()

    def test_broker_mem_obj_compress(self):
        self._broker_mem_obj(compress='snappy')

    def _broker_multi(self, count, compress=None):
        self.objs = dict()
        for i in range(count):
            d = dict()
            d['obj_id'] = uuid.uuid4()
            d['case_id'] = uuid.uuid4()
            d['parent_id'] = uuid.uuid4()
            d['data'] = self.object_data[:] + str(count)
            self.objs[i] = d

        self.offset = 0
        self.test_objs = dict()
        def test_func(proc, data_obj):
            self.test_objs[self.offset] = data_obj
            self.offset += 1

        pipelines = dict()
        pipelines['testing'] = TestPipeline('testing', test_func)
        self.assertTrue(self.process.load(force=True, pipelines=pipelines))
        self.process.transport.logger = FakeLogger()
        self.assertTrue(self.process.connect(force=True))

        conn = self.process.connection
        with conn.Producer(serializer='pickle',
                           compression=compress) as producer:
            for i in range(count):
                obj = self.objs[i]
                mem_obj = MemoryDataObject(
                    obj['obj_id'],
                    None,
                    obj['data'],
                    case_id=obj['case_id'],
                    name='opensource.svg',
                    path='/opensource.svg',
                    parent_id=obj['parent_id'],
                    type='file',
                    )
                producer.publish(mem_obj, exchange=self.process.exchange,
                    routing_key='gate.process', declare=[self.process.queue],
                    headers={'pipeline':'testing'})
        for _ in range(count):
            self.process.run_once()
        self.process.close()

        for i in range(count):
            obj = self.objs[i]
            test_obj = None
            for o in self.test_objs.values():
                if o.id == obj['obj_id']:
                    test_obj = o
                    break
            self.assertTrue(test_obj)
            self.assertEqual(obj['obj_id'], test_obj.id)
            self.assertEqual(obj['case_id'], test_obj.get('case_id'))
            self.assertEqual('opensource.svg', test_obj.get('name'))
            self.assertEqual('/opensource.svg', test_obj.get('path'))
            self.assertEqual(obj['parent_id'], test_obj.get('parent_id'))
            self.assertEqual('file', test_obj.get('type'))
            self.assertEqual(obj['data'], test_obj.read())

    def test_broker_multi_two(self):
        self._broker_multi(2)

    def test_broker_multi_three(self):
        self._broker_multi(3)

    def test_broker_multi_four(self):
        self._broker_multi(4)

    def test_broker_multi_two_compress(self):
        self._broker_multi(2, compress='snappy')

    def test_broker_multi_three_compress(self):
        self._broker_multi(3, compress='snappy')

    def test_broker_multi_four_compress(self):
        self._broker_multi(4, compress='snappy')

if __name__ == '__main__':
    unittest.main()
