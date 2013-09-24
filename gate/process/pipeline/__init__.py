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

from oslo.config import cfg
from gate.common import importutils
from gate.common.exceptions import GateException


cfg.CONF.register_opts([
    cfg.StrOpt('pipeline_driver',
               default='gate.process.common.pipeline.drivers.DirectoryPipelineDriver',
               help='Driver to use for loading the pipelines.'),
])


class PipelineError(GateException):
    pass


class Pipeline(object):

    """
    Main pipeline class, every driver should return this type.
    """

    def __init__(self, name):
        self.name = name
        self._initialized = False
        self._modules = list()

    def __getitem__(self, key):
        return self._modules[key]

    def __iter__(self):
        return self._modules.__iter__()

    def __reversed__(self):
        return self._modules.__reversed__()

    def __contains__(self, item):
        return self._modules.__contains__(item)

    def initialize(self):
        if not self._initialized:
            for mod in self:
                mod()
            self._initialized = True

    def append(self, module):
        if self._initialized:
            raise PipelineError('Cannot append module, pipeline already initialized.')
        self._modules.append(module)

    def process(self, bundle):
        self.initialize()
        bundle.reset()
        for mod in self._modules:
            bundle = mod.process(bundle)
            bundle.reset()
        return bundle


def get_pipeline_driver():
    return importutils.import_object(cfg.CONF.pipeline_driver)
