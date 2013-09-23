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

from oslo import messaging

from gate.common import log as logging


LOG = logging.getLogger(__name__)


class EngineClient(object):

    def __init__(self, transport, topic = 'engine'):
        self.topic = 'gate.%s' % topic
        self.target = messaging.Target(topic=self.topic, version='1.0')
        self._client = messaging.RPCClient(transport, self.target)

    def case_list(self, account_uuid):
        """List the cases.
        @param account_uuid: Account id
        @returns: List of cases
        """
        return self._client.call({}, 'case_list', account_uuid=account_uuid)

    def case_create(self, account_uuid, **kwargs):
        """Create a case.
        @param account_uuid: Account id
        @param **kwargs: Default key-value pairs
        @returns: Key-value pairs
        """
        kwargs['account_uuid'] = account_uuid
        return self._client.call({}, 'case_create', **kwargs)

    def case_get(self, case_uuid):
        """Get the case.
        @param case_uuid: Case id
        @returns: Key-value pairs
        """
        return self._client.call({}, 'case_get', case_uuid=case_uuid)

    def case_update(self, case_uuid, **kwargs):
        """Updates a case.
        @param case_uuid: Case id
        @param **kwargs: Key-value pairs to update
        @returns: Key-value pairs
        """
        kwargs['case_uuid'] = case_uuid
        return self._client.call({}, 'case_update', **kwargs)

    def case_delete(self, case_uuid):
        """Deletes a case.
        @param case_uuid: Case id
        @returns: True on success, false otherwise
        """
        return self._client.call({}, 'case_delete', case_uuid=case_uuid)

    def evidence_list(self, case_uuid):
        """List the evidence.
        @param case_uuid: Case id
        @returns: List of evidence
        """
        return self._client.call({}, 'evidence_list', case_uuid=case_uuid)

    def evidence_add(self, case_uuid, container_format, container_size, **kwargs):
        """Adds evidence to case.
        @param case_uuid: Case id
        @param container_format: Format of container
        @param container_size: Total size of container
        @param **kwargs: Default key-value pairs
        @returns: Key-value pairs
        """
        kwargs['case_uuid'] = case_uuid
        kwargs['container_format'] = container_format
        kwargs['container_size'] = container_size
        return self._client.call({}, 'evidence_add', **kwargs)

    def evidence_update(self, evidence_uuid, **kwargs):
        """Updates evidence.
        @param evidence_uuid: Evidence id
        @param **kwargs: Key-value pairs to update
        @returns: Key-value pairs
        """
        kwargs['evidence_uuid'] = evidence_uuid
        return self._client.call({}, 'evidence_update', **kwargs)

    def evidence_remove(self, evidence_uuid):
        """Removes evidence from case.
        @param evidence_uuid: Evidence id
        @returns: True on success, false otherwise
        """
        return self._client.call({}, 'evidence_remove', evidence_uuid=evidence_uuid)
