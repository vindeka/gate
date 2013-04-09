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

import hashlib
from gate.common.utils import get_logger


class HashModule(object):

    """
    Hashing module, provides hashing to all data that passes through the
    pipeline.
    """

    def __init__(self, conf):
        self.conf = conf
        self.logger = get_logger(conf, log_route='module.hash')

    def __call__(self):
        algos = self.conf.get('algorithms', None)
        if not algos:
            algos = 'md5 sha1 sha256 sha512'

        self.algos = []
        for a in algos.split(' '):
            self.algos.append(a.strip())

        self.logger.debug('Initialising hash module with algos: %s'
                          % algos)

    def process(self, proc, data_obj):
        for algo in self.algos:
            key = 'hash-%s' % algo
            if not data_obj.has_key(key):
                method = getattr(self, algo)
                data_obj.set(key, method(data_obj))
                data_obj.reset()

    def md5(self, data_obj):
        return self._hash(data_obj, hashlib.md5())

    def sha1(self, data_obj):
        return self._hash(data_obj, hashlib.sha1())

    def sha256(self, data_obj):
        return self._hash(data_obj, hashlib.sha256())

    def sha512(self, data_obj):
        return self._hash(data_obj, hashlib.sha512())

    def _hash(self, data_obj, hash):
        hash.update(data_obj.read())
        return hash.hexdigest()


def module_factory(conf):
    """Returns a module for use with gate."""

    return HashModule(conf)


