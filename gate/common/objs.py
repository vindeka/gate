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

import struct
import numpy

class DataObject(object):
    """
    Data object that is passed between the modules in a pipeline. Contains the
    attributes and the data for the given object.
    """

    def __init__(self, id, type, name, path, attrs = None):
        self.__dict__['attrs'] = {
            'id' : id,
            'type': type,
            'name' : name,
            'path' : path,
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

    def fileno(self):
        return self['id']

    def next(self):
        raise NotImplementedError

    def read(self, size = None):
        raise NotImplementedError

    def readline(self, size = None):
        raise NotImplementedError

    def readlines(self, sizehint = None):
        raise NotImplementedError

    def seek(self, offset, whence = 0):
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

    def readbytes(self, size = None):
        return numpy.fromstring(self.read(size), dtype='uint8')

class MemoryDataObject(DataObject):
    """
    Object holds the data for the object directly in memory, no access to actual
    file pointer is used.
    """

    def __init__(self, id, type, name, path, data, attrs = None):
        super(MemoryDataObject, self).__init__(id, type, name, path, attrs)

        self._data = data
        self._offset = 0

    def read(self, size = None):
        if size is None or size < 0:
            return self._data[self._offset:]
        total = self._offset + size
        return self._data[self._offset:total]

    def seek(self, offset, whence = 0):
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

class FileDataObject(DataObject):
    """
    Object reads the data for the object directly from disk. This is normally
    used for large files, where it is better to cache to disk.
    """

    def __init__(self, id, type, name, path, fp, opt_attrs = None):
        super(FileDataObject, self).__init__(id, type, name, path, opt_attrs)

        self._fp = fp

    def next(self):
        return self._fp.next()

    def read(self, size = None):
        if not size:
            return self._fp.read()
        return self._fp.read(size)

    def readline(self, size = None):
        if not size:
            return self._fp.readline()
        return self._fp.readline(size)

    def readlines(self, sizehint = None):
        if not sizehint:
            return self._fp.readlines()
        return self._fp.readlines(sizehint)

    def seek(self, offset, whence = 0):
        return self._fp.seek(offset, whence)

    def tell(self):
        return self._fp.tell()

    def reset(self):
        self._fp.seek(0, 0)

