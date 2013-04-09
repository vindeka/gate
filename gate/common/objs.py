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

""" Code for the objects used through out gate """

import os
import struct
import numpy
import tempfile


class DataObject(object):

    """
    Data object that is passed between the modules in a pipeline. Contains the
    attributes and the data for the given object.
    """

    def __init__(self, id, app, **kwargs):
        self.id = id
        self.app = app
        self.attrs = kwargs
        self.exceptions = list()

    def get(self, key, default = None):
        if key is 'id':
            return self.id
        return self.attrs.get(key, default)

    def set(self, key, value):
        self.attrs[key] = value

    def unset(self, key):
        del self.attrs[key]

    def has_key(self, key):
        return self.attrs.has_key(key)

    def keys(self):
        return self.attrs.keys()

    def iterkeys(self):
        return self.attrs.iterkeys()

    def items(self):
        return self.attrs.items()

    def iteritems(self):
        return self.attrs.iteritems()

    def size(self):
        size = None
        try:
            size = self['size']
        except:
            pass
        return size

    def next(self):
        raise NotImplementedError

    def read(self, size=None):
        raise NotImplementedError

    def readline(self, size=None):
        raise NotImplementedError

    def readlines(self, sizehint=None):
        raise NotImplementedError

    def seek(self, offset, whence=0):
        raise NotImplementedError

    def tell(self):
        raise NotImplementedError

    def reset(self):
        raise NotImplementedError

    def readbyte(self):
        return struct.unpack('B', self.read(1))[0]

    def readshort(self):
        return struct.unpack('H', self.read(2))[0]

    def readword(self):
        return struct.unpack('I', self.read(4))[0]

    def readquad(self):
        return struct.unpack('Q', self.read(8))[0]

    def readbytes(self, size=None):
        return numpy.fromstring(self.read(size), dtype='uint8')

    def close(self):
        pass

    def pack(self):
        """ Called before object is pushed to the queue. """
        self.app = None

    def unpack(self, app):
        """ Called after object is pulled from the queue. """
        self.app = app


class MemoryDataObject(DataObject):

    """
    Object holds the data for the object directly in memory, no access to actual
    file pointer is used.
    """

    def __init__(self, id, app, data = None, **kwargs):
        self._data = data
        self._offset = 0
        super(MemoryDataObject, self).__init__(id, app, **kwargs)

    def size(self):
        if not self._data:
            return 0
        return len(self._data)

    def read(self, size=None):
        if not self._data:
            return None

        if size is None or size < 0:
            return self._data[self._offset:]
        total = self._offset + size
        return self._data[self._offset:total]

    def seek(self, offset, whence=0):
        if not self._data:
            self.offset = 0
            return

        if whence == 0:
            self._offset = offset
        elif whence == 1:
            self._offset += offset
        elif whence == 2:
            self._offset = len(self._data) - offset

        if self._offset < 0:
            self._offset = 0
        elif self._offset > len(self._data):
            self._offset = len(self._data)

    def tell(self):
        return self._offset

    def reset(self):
        self._offset = 0

    def pack(self):
        self.reset()
        self.app = None

    def unpack(self, app):
        self.reset()
        self.app = app

class FileDataObject(DataObject):
    """
    Object reads the data for the object directly from disk. This is normally
    used for large files, where it is better to cache to disk.
    """

    def __init__(self, id, app, path, **kwargs):
        self._path = path
        self._fp = None
        super(FileDataObject, self).__init__(id, app, **kwargs)

    def _open(self):
        if not self._fp:
            self._fp = open(self._path, 'rb')

    def _close(self):
        if self._fp:
            self._fp.close()
            self._fp = None

    def next(self):
        self._open()
        return self._fp.next()

    def read(self, size = None):
        self._open()
        if not size:
            return self._fp.read()
        return self._fp.read(size)

    def readline(self, size = None):
        self._open()
        if not size:
            return self._fp.readline()
        return self._fp.readline(size)

    def readlines(self, sizehint = None):
        self._open()
        if not sizehint:
            return self._fp.readlines()
        return self._fp.readlines(sizehint)

    def seek(self, offset, whence = 0):
        self._open()
        return self._fp.seek(offset, whence)

    def tell(self):
        self._open()
        return self._fp.tell()

    def reset(self):
        self._open()
        self._fp.seek(0, 0)

    def pack(self):
        self._close()
        self.app = None

    def unpack(self, app):
        self.app = app
        self._open()


class URLDataObject(DataObject):

    """
    Object reads the data from url to temp file before read the data.
    """

    def __init__(self, id, app, url = None, **kwargs):
        self._url = url
        self._fp = None
        self._temp_path = None

        super(URLDataObject, self).__init__(id, app, **kwargs)

    def _open(self):
        """
        Opens the temp file for reading, calls download if not downloaded
        form large object transport.
        """

        if self._fp:
            return
        if not self._temp_path:
            self._temp_path = tempfile.mkstemp(prefix='glrg')
            self._download(self._temp_path)
        self._fp = open(self._temp_path, 'rb')

    def _download(self, path):
        """
        Downloads the large object from the large object transport into the provided path.
        """

        transport, key = self._url.split(':', limit = 1)
        if self.app.server.transport.name is not transport:
            raise Exception('Transport type not configured.')
        transport = self.app.server.transport
        stream = transport.get_stream(key)
        fp = open(self._temp_path, 'wb')
        while True:
            data = stream.read(4096)
            if data:
                fp.write(data)
        fp.close()

    def size(self):
        loc = self.tell()
        self._fp.seek(0, 2)
        size = self.tell()
        self._fp.seek(loc)
        return size

    def next(self):
        self._open()
        return self._fp.next()

    def read(self, size=None):
        self._open()
        if not size:
            return self._fp.read()
        return self._fp.read(size)

    def readline(self, size=None):
        self._open()
        if not size:
            return self._fp.readline()
        return self._fp.readline(size)

    def readlines(self, sizehint=None):
        self._open()
        if not sizehint:
            return self._fp.readlines()
        return self._fp.readlines(sizehint)

    def seek(self, offset, whence=0):
        self._open()
        return self._fp.seek(offset, whence)

    def tell(self):
        self._open()
        return self._fp.tell()

    def reset(self):
        self._open()
        self._fp.seek(0, 0)

    def pack(self):
        if self._fp:
            self._fp.close()
            self._fp = None
        if self._temp_path:
            if os.path.exists(self._temp_path):
                os.remove(self._temp_path)
            self._temp_path = None
        self.app = None

    def unpack(self, app):
        self.app = app
        self._open()


