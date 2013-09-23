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

import shutil
import unittest
import tempfile

from oslo.config import cfg
from gate.engine.common.storage import get_storage_driver
from gate.engine.common.storage.drivers import STOR_REG, StorageError, StorageDriverRegistryError
from gate.engine.common.storage.drivers.memory import MemoryDriver
from gate.engine.common.storage.drivers.level import LevelDBDriver

from test.unit.gate import BaseTestCase


class StorageDriverTestMixin(object):

    def _test_driver_create(self, driver):
        result = driver.create('testtype', area='testing', name='testname')
        
        self.assertValidId(result)

        result2 = driver.get('testtype', result['uuid'])

        self.assertEquals(result2['uuid'], result['uuid'])
        self.assertEquals(result2['area'], 'testing')
        self.assertEquals(result2['name'], 'testname')

    def _test_driver_update(self, driver):
        result = driver.create('testtype', area='testing', name='testname')

        self.assertValidId(result)

        result2 = driver.update('testtype', result['uuid'], area=None, name='newname')

        self.assertEquals(result2['uuid'], result['uuid'])
        self.assertTrue('area' not in result2)
        self.assertEquals(result2['name'], 'newname')

    def _test_driver_delete(self, driver):
        result = driver.create('testtype', area='testing', name='testname')
        
        self.assertValidId(result)

        result2 = driver.delete('testtype', result['uuid'])
        result3 = driver.get('testtype', result['uuid'])

        self.assertTrue(result2)
        self.assertTrue(result3 is None)

    def _test_driver_list(self, driver):
        one = driver.create('testtype', name='one')
        two = driver.create('testtype', name='two')
        
        self.assertValidId(one)
        self.assertValidId(two)

        items = driver.list('testtype')
        found = 0
        for item in items:
            if item['uuid'] == one['uuid']:
                found += 1
                self.assertEquals(item['name'], 'one')
            elif item['uuid'] == two['uuid']:
                found += 1
                self.assertEquals(item['name'], 'two')

        self.assertEquals(found, 2)

    def _test_driver_list_filter(self, driver):
        driver.ensure_index('testtype', 'name')
        one = driver.create('testtype', name='one')
        two = driver.create('testtype', name='two')
        
        self.assertValidId(one)
        self.assertValidId(two)

        items = driver.list('testtype', name='one')
        self.assertEquals(len(items), 1)
        self.assertEquals(items[0]['uuid'], one['uuid'])

    def _test_driver_index_miss(self, driver):
        one = driver.create('testtype', name='one')
        two = driver.create('testtype', name='two')
        
        self.assertValidId(one)
        self.assertValidId(two)

        self.assertRaises(StorageError, driver.list, 'testtype', name='one')


class StorageTest(BaseTestCase, StorageDriverTestMixin):

    def __init__(self, *args):
        cfg.CONF(args=[], project='gate', prog='engine-server')
        self.setupLogging()
        super(StorageTest, self).__init__(*args)

    def _get_temp_directory(self):
        return tempfile.mkdtemp()

    def assertValidId(self, result):
        self.assertTrue(result is not None)
        self.assertTrue('uuid' in result)
        self.assertTrue(result['uuid'] is not None)

    def test_storage_registry(self):
        self.assertTrue(hasattr(STOR_REG, '_drivers'))
        self.assertTrue('memory' in STOR_REG._drivers)
        self.assertEquals(STOR_REG._drivers['memory'], MemoryDriver)

    def test_storage_autoselect(self):
        driver = get_storage_driver('memory:///')
        self.assertEquals(driver.__class__.__name__, "MemoryDriver")

    def test_storage_autoselect_bad(self):
        self.assertRaises(StorageDriverRegistryError, get_storage_driver, 'baddriver:///')

    def test_storage_memory_create(self):
        driver = MemoryDriver('memory:///')
        self._test_driver_create(driver)

    def test_storage_memory_update(self):
        driver = MemoryDriver('memory:///')
        self._test_driver_update(driver)

    def test_storage_memory_delete(self):
        driver = MemoryDriver('memory:///')
        self._test_driver_delete(driver)

    def test_storage_memory_list(self):
        driver = MemoryDriver('memory:///')
        self._test_driver_list(driver)

    def test_storage_memory_list_filter(self):
        driver = MemoryDriver('memory:///')
        self._test_driver_list_filter(driver)

    def test_storage_memory_index_miss(self):
        driver = MemoryDriver('memory:///')
        self._test_driver_index_miss(driver)

    def test_storage_leveldb_create(self):
        tmpfile = self._get_temp_directory()
        driver_url = "leveldb://%s" % tmpfile
        driver = LevelDBDriver(driver_url)
        self._test_driver_create(driver)
        driver.close()
        shutil.rmtree(tmpfile)

    def test_storage_leveldb_update(self):
        tmpfile = self._get_temp_directory()
        driver_url = "leveldb://%s" % tmpfile
        driver = LevelDBDriver(driver_url)
        self._test_driver_update(driver)
        driver.close()
        shutil.rmtree(tmpfile)

    def test_storage_leveldb_delete(self):
        tmpfile = self._get_temp_directory()
        driver_url = "leveldb://%s" % tmpfile
        driver = LevelDBDriver(driver_url)
        self._test_driver_delete(driver)
        driver.close()
        shutil.rmtree(tmpfile)

    def test_storage_leveldb_list(self):
        tmpfile = self._get_temp_directory()
        driver_url = "leveldb://%s" % tmpfile
        driver = LevelDBDriver(driver_url)
        self._test_driver_list(driver)
        driver.close()
        shutil.rmtree(tmpfile)

    def test_storage_leveldb_list_filter(self):
        tmpfile = self._get_temp_directory()
        driver_url = "leveldb://%s" % tmpfile
        driver = LevelDBDriver(driver_url)
        self._test_driver_list_filter(driver)
        driver.close()
        shutil.rmtree(tmpfile)

    def test_storage_leveldb_index_miss(self):
        tmpfile = self._get_temp_directory()
        driver_url = "leveldb://%s" % tmpfile
        driver = LevelDBDriver(driver_url)
        self._test_driver_index_miss(driver)
        driver.close()
        shutil.rmtree(tmpfile)

if __name__ == '__main__':
    unittest.main()
