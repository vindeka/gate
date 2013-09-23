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
from oslo import messaging

from gate.common import log as logging
from gate.common.exceptions import GateException
from gate.engine.common.storage import get_storage_driver

LOG = logging.getLogger(__name__)


class EngineError(GateException):
    pass


class MissingKeyError(EngineError):
    pass


class EngineAPI(object):

    target = messaging.Target(version='1.0')

    def __init__(self, server):
        self.server = server
        self._storage_driver = server._storage_driver

    def case_list(self, *args, **kwargs):
        """List the cases.
        @param account_uuid: Account id
        @returns: List of cases
        """
        if 'account_uuid' not in kwargs:
            raise MissingKeyError('account_uuid')
        return self._storage_driver.list('case', **kwargs)

    def case_create(self, *args, **kwargs):
        """Create a case.
        @param account_uuid: Account id
        @param **kwargs: Default key-value pairs
        @returns: Key-value pairs
        """
        if 'account_uuid' not in kwargs:
            raise MissingKeyError('account_uuid')
        return self._storage_driver.create('case', **kwargs)

    def case_get(self, *args, **kwargs):
        """Get the case.
        @param case_uuid: Case id
        @returns: Key-value pairs
        """
        if 'case_uuid' not in kwargs:
            raise MissingKeyError('case_uuid')
        return self._storage_driver.get('case', kwargs['case_uuid'])

    def case_update(self, *args, **kwargs):
        """Updates a case.
        @param case_uuid: Case id
        @param **kwargs: Key-value pairs to update
        @returns: Key-value pairs
        """
        if 'case_uuid' not in kwargs:
            raise MissingKeyError('case_uuid')
        if 'account_uuid' in kwargs:
            del kwargs['account_uuid']
        uuid = kwargs['case_uuid']
        del kwargs['case_uuid']
        return self._storage_driver.update('case', uuid, **kwargs)

    def case_delete(self, *args, **kwargs):
        """Deletes a case.
        @param case_uuid: Case id
        @returns: True on success, false otherwise
        """
        if 'case_uuid' not in kwargs:
            raise MissingKeyError('case_uuid')
        uuid = kwargs['case_uuid']
        del kwargs['case_uuid']
        return self._storage_driver.delete('case', uuid)

    def evidence_list(self, *args, **kwargs):
        """List the evidence.
        @param case_uuid: Case id
        @returns: List of evidence
        """
        if 'case_uuid' not in kwargs:
            raise MissingKeyError('case_uuid')
        return self._storage_driver.list('evidence', kwargs['case_uuid'])

    def evidence_add(self, case_uuid=None, container_format=None, container_size=None, **kwargs):
        """Adds evidence to case.
        @param case_uuid: Case id
        @param container_format: Format of container
        @param container_size: Total size of container
        @param **kwargs: Default key-value pairs
        @returns: Key-value pairs
        """
        if not case_uuid:
            raise MissingKeyError('case_uuid')
        if not container_format:
            raise MissingKeyError('container_format')
        if not container_size:
            raise MissingKeyError('container_size')
        return self._storage_driver.create('evidence', case_uuid=case_uuid,
            container_format=container_format, container_size=container_size, **kwargs)

    def evidence_update(self, evidence_uuid, **kwargs):
        """Updates evidence.
        @param evidence_uuid: Evidence id
        @param **kwargs: Key-value pairs to update
        @returns: Key-value pairs
        """
        if not evidence_uuid:
            raise MissingKeyError('evidence_uuid')
        if 'case_uuid' in kwargs:
            del kwargs['case_uuid']
        if 'container_format' in kwargs and not kwargs['container_format']:
            raise MissingKeyError('container_format')
        if 'container_size' in kwargs and not kwargs['container_size']:
            raise MissingKeyError('container_size')
        return self._storage_driver.update('evidence', evidence_uuid=evidence_uuid, **kwargs)

    def evidence_remove(self, evidence_uuid):
        """Removes evidence from case.
        @param evidence_uuid: Evidence id
        @returns: True on success, false otherwise
        """
        return self._storage_driver.delete('evidence', evidence_uuid=evidence_uuid)


class EngineServer(object):

    def __init__(self, host, storage_driver=None, topic=None, allow_stop=False):
        topic = topic or 'engine'

        self.host = host
        self.topic = 'gate.%s' % topic

        self._storage_driver = storage_driver
        if not self._storage_driver:
            self._storage_driver = get_storage_driver()
        self._check_indexes()

        self._transport = messaging.get_transport(cfg.CONF)
        self._target = messaging.Target(topic=self.topic, server=self.host)
        self._endpoints = [
            EngineAPI(self)
        ]
        if allow_stop:
            self._endpoints.append(self)

        self._server = messaging.get_rpc_server(
            self._transport, self._target, self._endpoints)

    def start(self):
        LOG.info(
            '%s: Starting RPC server on target: %s.', self.host, self.topic)
        self._server.start()

    def wait(self):
        self._server.wait()

    def stop(self, ctx):
        LOG.info(
            '%s: Stopping RPC server on target: %s.', self.host, self.topic)
        self._server.start()
        self._server.stop()
        self._server.wait()

    def _check_indexes(self):
        self._storage_driver.ensure_index('case', 'account_uuid')
        self._storage_driver.ensure_index('evidence', 'case_uuid')
