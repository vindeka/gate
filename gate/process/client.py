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


class ProcessClient(object):

	def __init__(self, transport, topic = 'process'):
		self.topic = 'gate.%s' % topic
		self.target = messaging.Target(topic=self.topic, version='1.0')
		self._client = messaging.RPCClient(transport, self.target)

	def process_url(self, pipeline, data, url):
		"""Process the url using the given pipeline.
		@param pipeline: Name of pipeline to use
		@param data: Current information about the stream
		@param url: Url to identify the stream to load
		"""
		return self._client.call({}, 'process_url', pipeline=pipeline, data=data, url=url)

	def process_raw(self, pipeline, data, raw):
		"""Process the raw data using the given pipeline.
		@param pipeline: Name of pipeline to use
		@param data: Current information about the stream
		@param raw: Raw data to process
		"""
		return self._client.call({}, 'process_raw', pipeline=pipeline, data=data, raw=raw)

	