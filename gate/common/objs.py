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

    def __init__(
        self,
        app,
        case_id,
        id,
        type,
        name,
        path,
        attrs=None,
        ):
        self._app = app
        self.__dict__['attrs'] = {
            'case_id': case_id,
            'id': id,
            'type': type,
            'name': name,
            'path': path,
            }
        if attrs is not None:
            self.__dict__['attrs'].update(attrs)

    def __getattr__(self, name):
        v = self.__dict__['attrs'].get(name)
        if not v:
            raise AttributeError
        return v

    def __setattr__(self, name, value):
        self.__dict__['attrs'][name] = value

    def __delattr__(self, name):
        del self.__dict__['attrs'][name]

    def __dir__(self):
        keys = []
        for k in self.__dict__['attrs'].keys():
            if not k.startswith('_'):
                keys.append(k)
        return keys

    def __len__(self):
        return self.size()

    def fileno(self):
        return self['id']

    def size(self):
        size = None
        try:
            size = self['size']
        except:
            pass
        return size

    def copy(self):
        raise NotImplementedError

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

    def _close(self):
        pass


class MemoryDataObject(DataObject):

    """
    Object holds the data for the object directly in memory, no access to actual
    file pointer is used.
    """

    def __init__(
        self,
        app,
        case_id,
        id,
        type,
        name,
        path,
        data,
        attrs=None,
        ):
        super(MemoryDataObject, self).__init__(
            app,
            case_id,
            id,
            type,
            name,
            path,
            attrs,
            )

        self._data = data
        self._offset = 0

    def copy(self):
        c = MemoryDataObject(
            self.case_id,
            self.id,
            self.type,
            self.name,
            self.path,
            self._data.copy(),
            )
        for key in dir(self):
            setattr(c, key, getattr(self, key))
        return c

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

    def _close(self):
        self.reset()


class URLDataObject(DataObject):

    """
    Object reads the data either locally or transfers the file from an object
    store to a temp location. Protocol support is below:

        local: Reads the file from a local path
            ex: local:/tmp/file
        swift: Loads the data form a swift object store.
            ex: swift:container/object
    """

    def __init__(
        self,
        app,
        case_id,
        id,
        type,
        name,
        path,
        url,
        opt_attrs=None,
        ):
        super(URLDataObject, self).__init__(
            app,
            case_id,
            id,
            type,
            name,
            path,
            opt_attrs,
            )

        self._url = url
        self._fp = None
        self._temp_path = None

    def _open(self):
        if self._fp:
            return

        (url_type, path) = self._url_type()
        if url_type == 'local':
            self._fp = open(path, 'rb')
        elif url_type == 'swift':
            if not self._temp_path:
                self._temp_path = tempfile.mkstemp(prefix='gu')
                self._download(self._temp_path)
            self._fp = open(self._temp_path, 'rb')

    def _close(self):
        if self._fp:
            self._fp.close()
            self._fp = None
        if self._temp_path:
            os.remove(self._temp_path)
            self._temp_path = None

    def _download(self, path):
        """
        Downloads the large object from the swift url into the provided path.

            swift: Loads the data from a swift object store. Configuration needs to set
                   to provide url, tenant, account, password for the swift storage; this
                   information is not passed around.
                ex: swift:container/object
            local: Does nothing as file already exists locally.
        """

        if not self._url.startswith('swift:'):
            raise Exception('URL object downloading currently only supports swift object store.'
                            )

        url = self._url.replace('swift:', '')
        (container, obj) = url.split('/', maxsplit=1)
        if not container or not obj:
            raise Exception('Invalid swift url given: %s', url)

        (status, reason, headers, stream) = \
            get_object_stream(container, obj)
        if status != 200:
            raise Exception('Failed to read object from storage: (%d) %s %s'
                            , status, reason, url)

        fp = open(self._temp_path, 'wb')
        while True:
            data = stream.read(4096)
            if data:
                fp.write(data)
        fp.close()

    def copy(self):
        c = URLDataObject(
            self.case_id,
            self.id,
            self.type,
            self.name,
            self.path,
            self._url,
            )
        for key in dir(self):
            setattr(c, key, getattr(self, key))
        return c

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


