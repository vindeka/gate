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

import uuid

from gate.common.exceptions import GateException


class StorageError(GateException):
    pass


class StorageDriverBase(object):

    def __init__(self, storage_url):
        pass

    def __del__(self):
        self.close()

    def _generate_uuid(self):
        return str(uuid.uuid4())

    def list(self, type, **kwargs):
        """List the objects. Passed named parameters to filter on that value.
        @param type: object type
        @param **kwargs: filter arguments
        """
        raise NotImplemented()

    def get(self, type, uuid):
        """Get object by id.
        @param type: object type
        @param uuid: object id
        @returns: object
        """
        raise NotImplemented()

    def create(self, type, **kwargs):
        """Create object with given named paramaters.
        @param type: object type
        @param **kwargs: key-values save to object
        @returns: object
        """
        raise NotImplemented()

    def update(self, type, uuid, **kwargs):
        """Updates object with given named paramaters.
        @param type: object type
        @param uuid: object id
        @param **kwargs: key-values save to object
        @returns: object
        """
        raise NotImplemented()

    def delete(self, type, uuid):
        """Deletes object.
        @param type: object type
        @param uuid: object id
        @returns: True on success, false otherwise
        """
        raise NotImplemented()

    def has_index(self, type, key):
        """Test for index on object key.
        @param type: object type
        @param key: key in object
        @returns: True if key exists
        """
        raise NotImplemented()

    def ensure_index(self, type, key):
        """Creates index for key on object, if one is missing.
        @param type: object type
        @param key: key in object
        """
        raise NotImplemented()

    def get_container(self, uuid):
        """Gets container.
        @param uuid: container id
        @returns: StorageContainer object
        """
        raise NotImplemented()

    def create_container(self, uuid):
        """Creates a container, if container already exists it is replaced.
        @param uuid: container id
        @returns: StorageContainer object
        """
        raise NotImplemented()

    def delete_container(self, uuid):
        """Deletes container.
        @param uuid: container id
        @returns: True if success, false otherwise
        """
        raise NotImplemented()

    def close(self):
        """Closes the connection the driver makes."""
        raise NotImplemented()


class StorageDriverRegistryError(StorageError):
    pass


class StorageDriverRegistry(object):

    def __init__(self):
        self._drivers = dict()

    def register(self, key, klass):
        """Registers the class based on the url key.
        @param key: URL key
        @param klass: Driver class
        """
        if key in self._drivers:
            return
        self._drivers[key] = klass

    def _load_driver(self, storage_url):
        url_key = storage_url.split(':')[0]
        if url_key not in self._drivers:
            raise StorageDriverRegistryError(
                'Driver does not exists for the given storage_url.')
        klass = self._drivers[url_key]
        return klass(storage_url)


STOR_REG = StorageDriverRegistry()

__all__ = ["memory", "level"]
