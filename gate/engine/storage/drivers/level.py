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
import shutil
import leveldb

from oslo.config import cfg

from gate.common import log as logging
from gate.common import jsonutils
from gate.engine.storage.container import StorageContainer
from gate.engine.storage.drivers import StorageDriverBase, StorageError, STOR_REG

LOG = logging.getLogger(__name__)


cfg.CONF.register_opts([
    cfg.StrOpt('storage_leveldb_container_prefix',
               default='container_',
               help='Container database prefix.'),
])


class LevelDBDriver(StorageDriverBase):

    def __init__(self, storage_url):
        if not storage_url.startswith('leveldb:'):
            raise StorageError('Invalid storage url.')
        self.storage_url = storage_url
        self._path = self.storage_url.replace('leveldb://', '', 1)
        self._container_prefix = cfg.CONF.storage_leveldb_container_prefix
        self._db = leveldb.LevelDB(self._path)
        self._containers = dict()

    def _check_path(self):
        if not os.path.isdir(self._path):
            LOG.error('Storage URL is not a directory, so containers cannot be created.')
            return False
        return True

    def _container_path(self, key):
        if cfg.CONF.storage_leveldb_container_prefix:
            key = '%s%s' % (cfg.CONF.storage_leveldb_container_prefix, key)
        return os.path.join(self._path, key)

    def _get_key(self, type, uuid):
        return '%s.%s' % (type, uuid)

    def _dumps(self, obj):
        return jsonutils.dumps(obj)

    def _loads(self, str):
        return jsonutils.loads(str)

    def list(self, type, **kwargs):
        range_key = "%s." % type
        items = [self._loads(val) for key, val in self._db.RangeIter(key_from = range_key)]
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
        try:
            obj = self._db.Get(self._get_key(type, uuid))
        except:
            return None
        return self._loads(obj)

    def create(self, type, **kwargs):
        uuid = self._generate_uuid()
        key = self._get_key(type, uuid)
        kwargs['uuid'] = uuid
        self._db.Put(key, self._dumps(kwargs))
        return kwargs

    def update(self, type, uuid, **kwargs):
        kv = self.get(type, uuid)
        for key, value in kwargs.items():
            if value is None and key in kv:
                del kv[key]
            else:
                kv[key] = value
        kv['uuid'] = uuid
        self._db.Put(self._get_key(type, uuid), self._dumps(kv))
        return kv

    def delete(self, type, uuid):
        key = self._get_key(type, uuid)
        try:
            self._db.Get(key)
        except:
            return False
        self._db.Delete(key)
        return True

    def has_index(self, type, key):
        index_key = '__index.%s.%s' % (type, key)
        try:
            self._db.Get(index_key)
        except:
            return False
        return True

    def ensure_index(self, type, key):
        if self.has_index(type, key):
            return
        index_key = '__index.%s.%s' % (type, key)
        self._db.Put(index_key, '1')

    def get_container(self, uuid):
        if uuid in self._containers:
            return self._containers[uuid]
        if not self._check_path():
            return None
        path = self._container_path(uuid)
        if not os.path.exists(path):
            return None
        url = 'leveldb://%s' % path
        return StorageContainer(uuid, LevelDBDriver(url))

    def create_container(self, uuid):
        if not self._check_path():
            return None
        path = self._container_path(uuid)
        if os.path.exists(path):
            shutil.rmtree(path)
        os.mkdir(path)
        url = 'leveldb://%s' % path
        cont = StorageContainer(uuid, LevelDBDriver(url))
        self._containers[uuid] = cont
        return cont

    def delete_container(self, uuid):
        if uuid in self._containers:
            cont = self._containers[uuid]
            cont.close()
            del self._containers[uuid]
        if not self._check_path():
            return None
        path = self._container_path(uuid)
        if os.path.exists(path):
            shutil.rmtree(path)
            return True
        return False

    def close(self):
        del self._db
        self._db = None

STOR_REG.register('leveldb', LevelDBDriver)
