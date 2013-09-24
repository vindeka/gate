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
from gate.common import timeutils
from gate.common.exceptions import GateException
from gate.engine.storage import get_storage_driver
from gate.process.client import ProcessClient

LOG = logging.getLogger(__name__)


class EngineError(GateException):
    pass


class MissingKeyError(EngineError):
    pass


class EngineAPI(object):

    target = messaging.Target(version='1.0')

    def __init__(self, server, process_client=None):
        self.server = server
        self._storage_driver = server._storage_driver
        self._process_client = process_client

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
        kwargs['create_date'] = timeutils.strtime()
        kwargs['update_date'] = kwargs['create_date']
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

        kwargs['update_date'] = timeutils.strtime()
        return self._storage_driver.update('case', uuid, **kwargs)

    def case_delete(self, *args, **kwargs):
        """Deletes a case.
        @param case_uuid: Case id
        @returns: True on success, false otherwise
        """
        if 'case_uuid' not in kwargs:
            raise MissingKeyError('case_uuid')
        return self._storage_driver.delete('case', kwargs['case_uuid'])

    def evidence_list(self, *args, **kwargs):
        """List the evidence.
        @param case_uuid: Case id
        @returns: List of evidence
        """
        if 'case_uuid' not in kwargs:
            raise MissingKeyError('case_uuid')
        return self._storage_driver.list('evidence', **kwargs)

    def evidence_get(self, *args, **kwargs):
        """Get the evidence.
        @param evidence_uuid: Evidence id
        @returns: Key-value pairs
        """
        if 'evidence_uuid' not in kwargs:
            raise MissingKeyError('evidence_uuid')
        return self._storage_driver.get('evidence', kwargs['evidence_uuid'])

    def evidence_add(self, *args, **kwargs):
        """Adds evidence to case.
        @param case_uuid: Case id
        @param container_format: Format of container
        @param container_size: Total size of container
        @param **kwargs: Default key-value pairs
        @returns: Key-value pairs
        """
        if 'case_uuid' not in kwargs:
            raise MissingKeyError('case_uuid')
        if 'container_format' not in kwargs:
            raise MissingKeyError('container_format')
        if 'container_size' not in kwargs:
            raise MissingKeyError('container_size')

        kwargs['status'] = 'created'
        kwargs['create_date'] = timeutils.strtime()
        kwargs['update_date'] = kwargs['create_date']
        return self._storage_driver.create('evidence', **kwargs)

    def evidence_update(self, *args, **kwargs):
        """Updates evidence.
        @param evidence_uuid: Evidence id
        @param **kwargs: Key-value pairs to update
        @returns: Key-value pairs
        """
        if 'evidence_uuid' not in kwargs:
            raise MissingKeyError('evidence_uuid')
        if 'case_uuid' in kwargs:
            del kwargs['case_uuid']
        if 'container_format' in kwargs and not kwargs['container_format']:
            raise MissingKeyError('container_format')
        if 'container_size' in kwargs and not kwargs['container_size']:
            raise MissingKeyError('container_size')

        uuid = kwargs['evidence_uuid']
        del kwargs['evidence_uuid']
       
        kwargs['update_date'] = timeutils.strtime()
        item = self._storage_driver.update('evidence', uuid, **kwargs)

        return item

    def evidence_remove(self, *args, **kwargs):
        """Removes evidence from case.
        @param evidence_uuid: Evidence id
        @returns: True on success, false otherwise
        """
        if 'evidence_uuid' not in kwargs:
            raise MissingKeyError('evidence_uuid')
        return self._storage_driver.delete('evidence', kwargs['evidence_uuid'])

    def evidence_process(self, evidence_uuid, pipeline):
        """Starts the evidence processing, using the selected pipeline.
        @param evidence_uuid: Evidence id
        @param pipeline: Pipeline identifier
        """
        # Check for valid storage_url, this must be set before
        # processing can begin.
        item = self._storage_driver.get('evidence', evidence_uuid)
        if '_storage_url' not in item:
            raise MissingKeyError('_storage_url')

        # Get the evidence container in the storage driver to
        # hold the processing information for this piece of evidence.
        container = self._storage_driver.create_container(evidence_uuid)

        # Create object in container that maps to the evidence
        obj = container.create()

        # Update evidence information
        item = self._storage_driver.update('evidence', evidence_uuid, {
            'status': 'processing',
            'process_start_date': timeutils.strtime(),
            'process_pipeline': pipeline,
            '_root_uuid': obj['uuid']
        })

        # Start the processing
        self._process_client.process_url(evidence_uuid, pipeline, obj, item['storage_url'])

    def evidence_obj_save(self, evidence_uuid, data, obj_uuid=None):
        """Saves the object data for the piece of evidence, in the container
        for the evidence.
        @param evidence_uuid: evidence id
        @param data: object data
        @param obj_uuid: object id, or None if one not assigned
        @param returns: object id
        """
        container = self._storage_driver.get_container(evidence_uuid)
        if obj_uuid:
            obj = container.update(obj_uuid, **data)
        else:
            obj = container.create(**data)
        return obj['uuid']


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

        self._process_client = ProcessClient(self._transport)

        self._endpoints = [
            EngineAPI(self, process_client=self._process_client)
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
