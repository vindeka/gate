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
import pkg_resources
import gate.common.utils as utils
from ConfigParser import ConfigParser

class EggLoader(object):

    def __init__(self, egg):
        if egg.startswith('egg:'):
            egg = egg.replace('egg:', '')
        name = egg.split('#')[0]
        module = egg.split('#')[1]
        dist = pkg_resources.get_distribution(name)
        self.entry = dist.get_entry_info('gate.module_factory', module)

    def __call__(self):
        return self.entry.load()

class Pipeline(object):

    def __init__(self, name, conf_file):
        self.name = name
        self.objects = []
        self.inited = False
        config = ConfigParser()
        config.read([ conf_file ])
        pipeline = config.get('pipeline:main', 'pipeline')
        for p in pipeline.split(' '):
            p = p.strip()
            use = config.get('module:%s' % p, 'use')
            if use.startswith('egg:'):
                try:
                    loader = EggLoader(use)()
                except Exception, e:
                    raise ImportError('module:%s could not be loaded : %s' % (p, e))

                mod_conf = utils.readconf(conf_file, 'module:%s' % p)
                try:
                    l = loader(mod_conf)
                    self.objects.append(l)
                except Exception, e:
                    raise ImportError('module:%s failed to initialize : %s' % (p, e))

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

class Pipelines(object):

    def __init__(self, dir):
        self.pipelines = {};
        for file in glob.glob(os.path.join(dir, 'pipelines.d', '*.conf')):
            name = os.path.splitext(os.path.basename(file))[0]
            self.pipelines[name] = Pipeline(name, file)

    def __len__(self):
        return len(self.pipelines)

    def __getitem__(self, name):
        return self.get(name)

    def get(self, name):
        try:
            return self.pipelines[name]
        except:
            return None

