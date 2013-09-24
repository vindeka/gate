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
import glob

from oslo.config import cfg
from ConfigParser import ConfigParser
from gate.common import utils
from gate.process.common.pipeline import Pipeline


cfg.CONF.register_opts([
    cfg.StrOpt('pipelines_directory',
               default='/etc/gate/pipelines.d/',
               help='Path to load the pipelines from.'),
])


class DirectoryPipelineDriver(object):

    def __init__(self):
        self.pipelines = dict()
        path = cfg.CONF.pipelines_directory
        for filepath in glob.glob(os.path.join(path, '*.conf')):
            pipeline_name = os.path.splitext(os.path.basename(filepath))[0]
            self.pipelines[pipeline_name] = { 'path': filepath, 'object': None }

    def __getitem__(self, key):
        return self.get(key)

    def get(self, name):
        try:
            pipeline = self.pipelines[name]
        except:
            pipeline = None

        if not pipeline:
            return None

        if not pipeline['object']:
            obj = self._load(name, pipeline['path'])
            pipeline['object'] = obj
            return obj
        return pipeline['object']

    def _load(self, name, filepath):
        conf = ConfigParser()
        conf.read(filepath)

        pipeline = Pipeline()
        modules = conf.get('pipeline:main', 'pipeline')
        for mod in modules.split(' '):
            mod = mod.strip()           
            use = conf.get('module:%s' % mod, 'use')
            if use.startswith('egg:'):
                try:
                    loader = utils.load_egg(use, 'gate.module_factory')
                except Exception as e:
                    raise ImportError('module:%s could not be loaded : %s'
                             % (mod, e))

                mod_conf = utils.readconf(filepath, 'module:%s' % mod)
                try:
                    l = loader(mod_conf)
                    self.pipeline.append(l)
                except Exception as e:
                    raise ImportError('module:%s failed to initialize : %s'
                             % (mod, e))
        return pipeline
