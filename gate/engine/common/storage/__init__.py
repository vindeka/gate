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

from oslo.config import cfg
from gate.common import importutils

from gate.engine.common.storage.drivers import STOR_REG
from gate.engine.common.storage.drivers import *


cfg.CONF.register_opts([
    cfg.StrOpt('storage_url',
               default='sqlite:////etc/gate/storage.db',
               help='Connection URL to backend storage.'),
    cfg.StrOpt('storage_driver',
               default=None,
               help='Driver to use to connect to backend storage.'),
])


def get_storage_driver(storage_url=None):
    if not storage_url:
        storage_url = cfg.CONF.storage_url

    if not cfg.CONF.storage_driver:
        return STOR_REG._load_driver(storage_url)
    return importutils.import_object(cfg.CONF.storage_driver, storage_url)
