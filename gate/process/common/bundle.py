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


class Bundle(object):

    """
    Bundle is passed through the pipeline, contains all information needed to process.
    """

    def __init__(self, server, pipeline, data, stream):
        """initialize the bundle.
        @param server: RPC Server instance
        @param pipeline: Pipeline processing the object
        @param data: Dictionary of keys for the item
        @param stream: Raw stream of data
        """
        self.server = server
        self.pipeline = pipeline
        self.data = data
        self.stream = stream

    def reset(self):
        """Resets the stream to beginning for next module in the pipeline to process."""
        if self.stream:
            if hasattr(self.stream, 'seek'):
                self.stream.seek(0)
            elif hasattr(self.stream, 'reset'):
                self.stream.reset()

    def add_exception(self, exc, traceback=None):
        """Adds an exceptions to the bundle data."""
        if 'exceptions' not in self.data:
            self.data['exceptions'] = list()
        self.data['exceptions'].append({
            'name': exc.__class__.__name__,
            'msg': str(exc),
            'traceback': traceback
        })
