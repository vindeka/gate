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
from swiftly.client import Client
from swiftly.concurrency import Concurrency
from gate.common.utils import get_logger


class SwiftTransport(object):

    name = 'swift'

    def __init__(self, conf):
        self.conf = conf
        self.logger = get_logger(conf, log_route='transport.memcached')
        self.auth_url = conf.get('auth_url', 'http://localhost:5000/')
        self.auth_user = conf.get('auth_user', 'gate')
        self.auth_key = conf.get('auth_key', 'gate')
        self.proxy = conf.get('proxy', 'None')
        if self.proxy == 'None':
            self.proxy = None
        self.retries = int(conf.get('retries', '4'))
        self.swift_proxy = conf.get('swift_proxy', 'None')
        if self.swift_proxy == 'None':
            self.swift_proxy = None
        self.cache_path = conf.get('cache_path',
                                   '/var/gate/object-store/cache')
        dir_path = os.path.dirname(os.path.abspath(cache_path))
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        self.concurrency = int(conf.get('put_threads', 5))
        self.segment_size = int(conf.get('segment_size', 262144000))

    def open(self):
        self.client = Client(
            auth_url,
            auth_user,
            auth_key,
            proxy=proxy,
            retries=retries,
            swift_proxy=swift_proxy,
            cache_path=cache_path,
            )
        try:
            (status, reason, headers, contents) = \
                __swiftly_client.head_account()
        except:
            self.logger.error('Connection failed: connection refused.')
            return False
        if status != 200:
            self.logger.error('Connection failed: (%d) %s' % (status,
                              reason))
            return False
        self.logger.debug('Connection made to swift.')
        return True

    def close(self):
        self.client.close()
        self.logger.debug('Connection closed.')

    def get_stream(self, key):
        (container, obj) = key.split('/', limit=1)
        return self.client.get_object(container, obj, stream=True)

    def put_stream(self, key, stream):
        (container, obj) = key.split('/', limit=1)
        size = None
        func_size = getattr(stream, 'size', None)
        if func_size:
            size = func_size()
        if not size:
            return self.client.client.put_object(container, obj, stream)
        if size <= self.segment_size:
            headers = {'Content-Length': size}
            (status, reason, headers, contents) = \
                self.client.put_object(container, obj, stream,
                    headers=headers)
            if status != 201:
                self.logger.error('Failed to put object: %s/%s'
                                  % (container, obj))
                return False
        cont_prefix = '%s_segments' % container
        prefix = '%s/%s/' % (obj, self.segment_size)
        conc = Concurrency(self.concurrency)
        start = 0
        segment = 0
        while start < size:
            obj_path = '%s%08d' % (prefix, segment)
            conc.spawn(
                segment,
                self._put_recursive,
                cont_prefix,
                obj_path,
                stream.copy(),
                start,
                )
            for rv in conc.get_results().values():
                if not rv:
                    conc.join()
                    self.logger.error('Failed to create segments for object: %s/%s'
                             % (container, obj))
                    return rv
            segment += 1
            start += self.client.segment_size
        conc.join()
        for rv in conc.get_results().values():
            if not rv:
                conc.join()
                self.logger.error('Failed to create segments for object: %s/%s'
                                   % (container, obj))
                return rv
        headers = {'Content-Length': 0, 'X-Object-Manifest': '%s/%s' \
                   % (cont_prefix, prefix)}
        (status, reason, headers, contents) = \
            self.client.put_object(container, obj, '', headers=headers)
        if status != 201:
            self.logger.error('Failed put manifest object: %s/%s'
                              % (container, obj))
            return False
        return True

    def _put_recursive(
        container,
        obj,
        stream,
        offset,
        size,
        ):
        stream.seek(offset)
        headers = {'Content-Length': size}
        try:
            (status, reason, headers, contents) = \
                self.client.put_object(container, obj, stream,
                    headers=headers)
            if status != 201:
                return False
        except:
            return False
        return True


def transport_factory(conf):
    """Returns a transport for use with gate."""

    return SwiftTransport(conf)


