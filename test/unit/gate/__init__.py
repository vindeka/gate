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

from oslo.config import cfg

from gate.common import log as logging
from gate.process.pipeline import Pipeline


class FakeModule(object):

    def __init__(self, func):
        self.func = func

    def __call__(self):
        pass

    def process(self, bundle):
        return self.func(bundle)


class FakePipeline(Pipeline):

    def __init__(self, name, func=None):
        super(FakePipeline, self).__init__(name)
        self._initialized = True
        self.add_func(func)

    def add_func(self, func):
        if not func:
            return
        self._initialized = False
        self.append(FakeModule(func))
        self._initialized = True

    def add_module(self, module):
        self._initialized = False
        module()
        self.append(module)
        self._initialized = True


class FakePipelineDriver(object):

    def __init__(self):
        self.pipelines = dict()

    def __getitem__(self, key):
        return self.get(key)

    def get(self, name):
        try:
            pipeline = self.pipelines[name]
        except:
            pipeline = None

        if not pipeline:
            return None

        return pipeline

    def add_pipeline(self, pipeline, name=None):
        if not name:
            name = pipeline.name

        self.pipelines[name] = pipeline


class BaseTestCase(unittest.TestCase):

    def enableFakeTransport(self):
        cfg.CONF.transport_driver = 'fake'
        cfg.CONF.transport_url = 'fake:'

    def setupLogging(self):
        cfg.CONF.default_log_levels = ['stevedore=WARN', 'gate=WARN']
        logging.setup('gate')
