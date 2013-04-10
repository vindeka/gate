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

import logging
from sys import exc_info
from collections import defaultdict
from gate.common.pipeline import Pipeline

class FakeModule(object):

    def __init__(self, func):
        self.func = func

    def __call__(self):
        pass

    def process(self, proc, data_obj):
        self.func(proc, data_obj)

class FakePipeline(Pipeline):

    def __init__(self, name, func=None):
        self.name = name
        self.objects = []
        self.inited = False
        if func:
            self.add_module(FakeModule(func))

    def add_module(self, module):
        self.objects.append(module)

class FakeLogger(object):
    # a thread safe logger

    def __init__(self, *args, **kwargs):
        self._clear()
        self.level = logging.NOTSET
        if 'facility' in kwargs:
            self.facility = kwargs['facility']

    def _clear(self):
        self.log_dict = defaultdict(list)

    def _store_in(store_name):
        def stub_fn(self, *args, **kwargs):
            self.log_dict[store_name].append((args, kwargs))
        return stub_fn

    error = _store_in('error')
    info = _store_in('info')
    warning = _store_in('warning')
    debug = _store_in('debug')

    def exception(self, *args, **kwargs):
        self.log_dict['exception'].append((args, kwargs, str(exc_info()[1])))

    # mock out the StatsD logging methods:
    increment = _store_in('increment')
    decrement = _store_in('decrement')
    timing = _store_in('timing')
    timing_since = _store_in('timing_since')
    update_stats = _store_in('update_stats')
    set_statsd_prefix = _store_in('set_statsd_prefix')

    def get_increments(self):
        return [call[0][0] for call in self.log_dict['increment']]

    def get_increment_counts(self):
        counts = {}
        for metric in self.get_increments():
            if metric not in counts:
                counts[metric] = 0
            counts[metric] += 1
        return counts

    def setFormatter(self, obj):
        self.formatter = obj

    def close(self):
        self._clear()

    def set_name(self, name):
        # don't touch _handlers
        self._name = name

    def acquire(self):
        pass

    def release(self):
        pass

    def createLock(self):
        pass

    def emit(self, record):
        pass

    def handle(self, record):
        pass

    def flush(self):
        pass

    def handleError(self, record):
        pass
