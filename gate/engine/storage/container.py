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


class StorageContainer(object):

    def __init__(self, uuid, storage_backend):
        self.id = uuid
        self.uuid = uuid
        self._storage_backend = storage_backend

    def __del__(self):
        self.close()

    def get(self, obj_uuid):
        return self._storage_backend.get('obj', obj_uuid)

    def list(self, **kwargs):
        return self._storage_backend.list('obj', **kwargs)

    def create(self, **kwargs):
        return self._storage_backend.create('obj', **kwargs)

    def update(self, obj_uuid, **kwargs):
        return self._storage_backend.update('obj', obj_uuid, **kwargs)

    def delete(self, obj_uuid):
        return self._storage_backend.delete('obj', obj_uuid)

    def has_index(self, key):
        return self._storage_backend.has_index('obj', key)

    def ensure_index(self, key):
        return self._storage_backend.ensure_index('obj', key)

    def close(self):
        return self._storage_backend.close()
