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

from gate.common.utils import get_logger


class DebugModule(object):

    def __init__(self, conf):
        self.conf = conf
        self.logger = get_logger(conf, log_route='module.debug')

    def __call__(self):
        self.logger.debug('Initialising debug module')

    def process(self, proc, data_obj):
        print '%d:%s %s (%s)' % (data_obj.id, data_obj.get('type'),
                                 data_obj.get('name'), data_obj.get('path'))
        for key in data_obj.iterkeys():
            print '\t%s: %s' % (key, data_obj.get(key))


def module_factory(conf):
    """Returns a module for use with gate."""

    return DebugModule(conf)


