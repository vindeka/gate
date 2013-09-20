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


from gate.common import log as logging


LOG = logging.getLogger(__name__)


class HashModule(object):

    """
    Hashing module, provides hashing to all data that passes through the
    pipeline.
    """

    def __init__(self, conf):
        self.conf = conf

    def __call__(self):
        self.prefix = self.conf.get('prefix', None)
        algos = self.conf.get('algorithms', None)
        if not algos:
            algos = 'md5 sha1 sha256 sha512'

        self.algos = []
        for a in algos.split(' '):
            self.algos.append(a.strip())

        LOG.debug('Initialising hash module with algos: %s'
                  % algos)

    def process(self, bundle):
        for algo in self.algos:
            if self.prefix:
                key = '%s-hash-%s' % (self.prefix, algo)
            else:
                key = 'hash-%s' % algo
            if key not in bundle.data:
                method = getattr(self, algo)
                bundle.data[key] = method(bundle)
                bundle.reset()

    def md5(self, bundle):
        return self._hash(bundle, hashlib.md5())

    def sha1(self, bundle):
        return self._hash(bundle, hashlib.sha1())

    def sha256(self, bundle):
        return self._hash(bundle, hashlib.sha256())

    def sha512(self, bundle):
        return self._hash(bundle, hashlib.sha512())

    def _hash(self, bundle, hash):
        if bundle.stream:
            hash.update(bundle.stream.read())
        return hash.hexdigest()


def module_factory(conf):
    return HashModule(conf)
