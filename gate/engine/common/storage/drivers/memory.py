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


from gate.common import log as logging
from gate.engine.common.storage.drivers import StorageDriverBase, StorageError, STOR_REG

LOG = logging.getLogger(__name__)


class MemoryDriver(StorageDriverBase):

    def __init__(self, storage_url):
        if not storage_url.startswith('memory:'):
            LOG.error('Invalid storage url.')
            raise StorageError('Invalid storage url.')
        self.storage_url = storage_url
        self._storage = dict()
        self._index = dict()

    def _get_or_create_type(self, type):
        if type not in self._storage:
            self._storage[type] = dict()
        return self._storage[type]

    def list(self, type, **kwargs):
        type_db = self._get_or_create_type(type)
        items = type_db.values()
        for key, value in kwargs.items():
            if not self.has_index(type, key):
                raise StorageError('Cannot filter, missing index: %s' % key)

            valid = list()
            for item in items:
                if key in item and value is None:
                    valid.append(item)
                elif key in item and value == item[key]:
                    valid.append(item)
            items = valid
        return items

    def get(self, type, uuid):
        type_db = self._get_or_create_type(type)
        if uuid not in type_db:
            return None
        return type_db[uuid]

    def create(self, type, **kwargs):
        uuid = self._generate_uuid()
        type_db = self._get_or_create_type(type)
        while uuid in type_db:
            uuid = self._generate_uuid()
        kwargs['uuid'] = uuid
        type_db[uuid] = kwargs
        return kwargs

    def update(self, type, uuid, **kwargs):
        type_db = self._get_or_create_type(type)
        if uuid not in type_db:
            return False
        kv = type_db[uuid]
        for key, value in kwargs.items():
            if value is None and key in kv:
                del kv[key]
            else:
                kv[key] = value
        kv['uuid'] = uuid
        type_db[uuid] = kv
        return kv

    def delete(self, type, uuid):
        type_db = self._get_or_create_type(type)
        if uuid not in type_db:
            return False
        del type_db[uuid]
        return True

    def has_index(self, type, key):
        index_key = '%s.%s' % (type, key)
        return index_key in self._index

    def ensure_index(self, type, key):
        if self.has_index(type, key):
            return
        index_key = '%s.%s' % (type, key)
        self._index[index_key] = True

    def close(self):
        self._storage = None
        self._index = None


STOR_REG.register('memory', MemoryDriver)
