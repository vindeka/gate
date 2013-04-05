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
from swiftly.client import Client
from swiftly.concurrency import Concurrency

__swiftly_client = None

def init_swift(conf):
	"""
	Intializes the connection of the swift object storage.

	:param conf: Dictionary of key, value from configuration file
		for the object-store section.
	"""
	global __swiftly_client
	if __swiftly_client:
		raise Exception('Object storage already initialized.')
	auth_url = conf.get('auth_url', 'http://localhost:5000/')
	auth_user = conf.get('auth_user', 'gate')
	auth_key = conf.get('auth_key', 'gate')
	proxy = conf.get('proxy', 'None')
	if proxy == 'None':
		proxy = None
	retries = int(conf.get('retries', '4'))
	swift_proxy = conf.get('swift_proxy', 'None')
	if swift_proxy == 'None':
		swift_proxy = None
	cache_path = conf.get('cache_path', '/var/gate/object-store/cache')
	dir_path = os.path.dirname(os.path.abspath(cache_path)) 
	if not os.path.exists(dir_path):
		os.makedirs(dir_path)
	__swiftly_client = Client(auth_url, auth_user, auth_key, proxy = proxy,
		retries = retries, swift_proxy = swift_proxy, cache_path = cache_path)
	try:
		status, reason, headers, contents = __swiftly_client.head_account()
	except:
		raise Exception('Object storage initialization failed: connection refused.')
	if status != 200:
		raise Exception('Object storage initialization failed: (%d) %s' % (status, reason))
	__swiftly_client.concurrency = int(conf.get('put_threads', 5))
	__swiftly_client.segment_size = int(conf.get('segment_size', 262144000))

def get_object_stream(container, obj, headers = None):
	"""
	GETs the object from the object store.

	:param container: The name of the container.
	:param obj: The name of the object.

	:returns: A tuple of (status, reason, headers, stream).
		:status: is an int for the HTTP status code.
		:reason: is the str for the HTTP status (ex: "Ok").
		:headers: is a dict with all lowercase keys of the HTTP
			headers; if a header has multiple values, it will be a
			list.
		:contents: file-like-object of the contents of the HTTP body.
	"""
	global __swiftly_client
	if not __swiftly_client:
		raise Exception('Object storage not initialized.')
	return __swiftly_client.get_object(container, obj, headers = headers, stream = True)

def put_object_stream(container, obj, stream, headers = None):
	"""
	PUTs the object and returns the results.

	:param container: The name of the container.
	:param obj: The name of the object.
	:param stream: File-like-object with at least a read function.
	:param headers: Additional headers to send with the request.
	:returns: A tuple of (status, reason, headers, contents).

	    :status: is an int for the HTTP status code.
	    :reason: is the str for the HTTP status (ex: "Ok").
	    :headers: is a dict with all lowercase keys of the HTTP
	        headers; if a header has multiple values, it will be a
	        list.
	    :contents: is the str for the HTTP body.
	"""
	global __swiftly_client
	if not __swiftly_client:
		raise Exception('Object storage not initialized.')
	size = None
	func_size = getattr(stream, 'size', None)
	if func_size:
		size = func_size()
	if not size:
		return __swiftly_client.put_object(container, obj, stream, headers = headers)
	if size <= __swiftly_client.segment_size:
		if not headers:
			headers = dict()
		headers.update({
			'Content-Length': size
		})
		return __swiftly_client.put_object(container, obj, stream, headers = headers)
	cont_prefix = '%s_segments' %  container
	prefix = '%s/%s/' % (obj, __swiftly_client.segment_size)
	conc = Concurrency(__swiftly_client.concurrency)
	start = 0
	segment = 0
	while start < size:
		obj_path = '%s%08d' % (prefix, segment)
		conc.spawn(segment, _put_recursive, cont_prefix, obj_path,
			stream.copy(), start)
		for rv in conc.get_results().values():
			if rv[0] != 200:
				conc.join()
				return rv
		segment += 1
		start += __swiftly_client.segment_size
	conc.join()
	for rv in conc.get_results().values():
		if rv[0] != 200:
			conc.join()
			return rv
	if not headers:
		headers = dict()
	headers.update({
		'Content-Length': 0,
		'X-Object-Manifest': '%s/%s' % (cont_prefix, prefix),
	})
	return __swiftly_client.put_object(container, obj, '', headers = headers)

def _put_recursive(container, obj, stream, offset, size):
	stream.seek(offset)
	headers = {
		'Content-Length': size
	}
	try:
		return __swiftly_client.put_object(container, obj, stream, headers = headers)
	except:
		return (500, 'Internal application error in recursion', [], '')
