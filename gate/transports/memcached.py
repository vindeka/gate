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

import os
import memcache
import cPickle as pickle
from gate.common.utils import get_logger, config_true_value


def md5hash(key):
    return md5(key).hexdigest()


class MemcachedStream(object):

    def __init__(self, key, client):
        self.meta = self._getmeta(key)
        if not meta:
            raise Exception('Failed to retrieve metadata for key: %s'
                            % key)
        self.key = key
        self.client = client
        self.total_size = meta.get('size', None)
        self.segment_size = meta.get('segment_size', None)
        self.segments = meta.get('segments', None)
        self.compression = meta.get('compression', None)
        if not self.size or not self.segment_size or not self.segments:
            raise Exception('Invalid metadata for key: %s' % key)
        self.offset = 0
        self._seg = None
        self._seg_current = -1
        self._seg_offset = 0
        sekf._seg_size = 0

    def __len__(self):
        return self.size()

    def size(self):
        return self.total_size

    def read(self, size=None):
        if self._seg_current < 0:
            self._seg = self._getsegment(0)
            self._seg_current = 0
            self._seg_offset = 0
            self._seg_size = len(self._seg)

        s = size
        if not s:
            s = self.total_size
        i = self.offset
        data = ''
        while i < s:
            if not self._seg:
                self._seg = self._getsegment(self._seg_current)
                self._seg_size = len(self._seg)
            data += self._seg[self._seg_offset]
            self._seg_offset += 1
            i += 1
            if self._seg_offset >= self._seg_size:
                if self._seg_current < self.segments:
                    self._seg_current += 1
                    self._seg_offset = self._seg_size = 0
        self.offset = i
        return data

    def tell(self):
        return self.offset

    def _getmeta(self, key):
        obj = self.client.get(md5hash(key))
        if not obj:
            return obj
        return pickle.loads(obj)

    def _getsegment(self, segment):
        return self.client.get(md5hash('%s/%d' % (self.key, segment)))


class MemcachedTransport(object):

    name = 'memcached'

    def __init__(self, conf):
        self.conf = conf
        self.logger = get_logger(conf, log_route='transport.memcached')
        self.servers = conf.get('servers', 'localhost:11211').split(';')
        self.debug = config_true_value(conf.get('debug', 'False'))

    def open(self):
        self.client = memcache.Client(self.servers, debug=self.debug)
        self.logger.debug('Connection made to servers: %s.'
                          % ';'.join(self.servers))
        return True

    def close(self):
        self.client.close()
        self.logger.debug('Connection closed.')

    def get_stream(self, key):
        return MemcachedStream(key, client)

    def put_stream(self, key, stream):
        pass


def transport_factory(conf):
    """Returns a transport for use with gate."""

    return MemcachedTransport(conf)


