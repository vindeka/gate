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

""" Code for the pipeline system """

import os
import glob
import gate.common.utils as utils
from ConfigParser import ConfigParser


class Pipeline(object):

    def __init__(self, name, conf_file):
        self.name = name
        self.objects = []
        self.inited = False
        config = ConfigParser()
        config.read([conf_file])
        pipeline = config.get('pipeline:main', 'pipeline')
        for p in pipeline.split(' '):
            p = p.strip()
            use = config.get('module:%s' % p, 'use')
            if use.startswith('egg:'):
                try:
                    loader = utils.load_egg(use, 'gate.module_factory')
                except Exception, e:
                    raise ImportError('module:%s could not be loaded : %s'
                             % (p, e))

                mod_conf = utils.readconf(conf_file, 'module:%s' % p)
                try:
                    l = loader(mod_conf)
                    self.objects.append(l)
                except Exception, e:
                    raise ImportError('module:%s failed to initialize : %s'
                             % (p, e))

    def initialize(self):
        if not self.inited:
            for o in self.objects:
                o()
            self.inited = True

    def process(self, proc, data_obj):
        self.initialize()
        data_obj.reset()
        for o in self.objects:
            o.process(proc, data_obj)
            data_obj.reset()

def load_pipelines(dir):
    pipelines = dict()
    for file in glob.glob(os.path.join(dir, 'pipelines.d', '*.conf')):
        name = os.path.splitext(os.path.basename(file))[0]
        pipelines[name] = Pipeline(name, file)
    return pipelines
