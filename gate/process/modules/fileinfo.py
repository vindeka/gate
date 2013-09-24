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
import magic


from gate.common import log as logging


LOG = logging.getLogger(__name__)


class FileInfoModule(object):

    """
    Provides basic file information.
    """

    def __init__(self, conf):
        self.conf = conf

    def __call__(self):
        self._filetype = magic.Magic()
        self._mimetype = magic.Magic(mime=True)
        LOG.debug('Initialized.')

    def process(self, bundle):
        bundle.data['filetype'] = self._filetype.from_buffer(bundle.stream.read())
        bundle.reset()
        bundle.data['mimetype'] = self._mimetype.from_buffer(bundle.stream.read())
        bundle.reset()
        bundle.data['filesize'] = len(bundle.stream.read())

        if 'path' in bundle.data:
            ext = os.path.splitext(bundle.data['path'])[1]
            bundle.data['extension'] = ext[1:]


def module_factory(conf):
    return FileInfoModule(conf)
