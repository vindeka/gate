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

# NOTE: gate_conn
# You'll see gate_conn passed around a few places in this file. This is the
# source httplib connection of whatever it is attached to.
#   It is used when early termination of reading from the connection should
# happen, such as when a range request is satisfied but there's still more the
# source connection would like to send. To prevent having to read all the data
# that could be left, the source connection can be .close() and then reads
# commence to empty out any buffers.
#   These shenanigans are to ensure all related objects can be garbage
# collected. We've seen objects hang around forever otherwise.

import os
import uuid
import mimetypes

from oslo.config import cfg

from random import shuffle
from time import time
from eventlet import Timeout
from gate.common.utils import get_logger, split_path, \
    config_true_value, check_utf8
from gate.controllers import CaseController
#, EvidenceController, \
#    ObjectController
from gate.common.swob import HTTPBadRequest, HTTPForbidden, \
    HTTPMethodNotAllowed, HTTPNotFound, HTTPPreconditionFailed, \
    HTTPServerError, Request


class Application(object):
    """WSGI application for the api server."""

    def __init__(self, conf, memcache=None, logger=None, account_ring=None,
                 container_ring=None, object_ring=None):
        if conf is None:
            conf = {}
        if logger is None:
            self.logger = get_logger(conf, log_route='api-server')
        else:
            self.logger = logger

        gate_dir = conf.get('gate_dir', '/etc/gate')
        self.client_timeout = int(conf.get('client_timeout', 60))
        self.put_queue_depth = int(conf.get('put_queue_depth', 10))
        self.object_chunk_size = int(conf.get('object_chunk_size', 65536))
        self.client_chunk_size = int(conf.get('client_chunk_size', 65536))
        self.error_suppression_interval = \
            int(conf.get('error_suppression_interval', 60))
        self.error_suppression_limit = \
            int(conf.get('error_suppression_limit', 10))
        self.deny_host_headers = [
            host.strip() for host in
            conf.get('deny_host_headers', '').split(',') if host.strip()]
        self.rate_limit_after_segment = \
            int(conf.get('rate_limit_after_segment', 10))
        self.rate_limit_segments_per_sec = \
            int(conf.get('rate_limit_segments_per_sec', 1))

    def get_controller(self, path):
        """
        Get the controller to handle a request.

        :param path: path from request
        :returns: tuple of (controller class, path dictionary)

        :raises: ValueError (thrown by split_path) if given invalid path
        """
        version, case, evidence, obj = split_path(path, 1, 4, True)
        d = dict(version=version,
                 case_name=case,
                 evidence_name=evidence,
                 obj_name=obj)
        if obj and evidence and case:
            return ObjectController, d
        elif evidence and case:
            return ContainerController, d
        elif case and not container and not obj:
            return CaseController, d
        return None, d

    def __call__(self, env, start_response):
        """
        WSGI entry point.
        Wraps env in swob.Request object and passes it down.

        :param env: WSGI environment dictionary
        :param start_response: WSGI callable
        """
        try:
            req = Request(env)
            return self.handle_request(req)(env, start_response)
        except UnicodeError:
            err = HTTPPreconditionFailed(
                request=req, body='Invalid UTF8 or contains NULL')
            return err(env, start_response)
        except (Exception, Timeout):
            start_response('500 Server Error',
                           [('Content-Type', 'text/plain')])
            return ['Internal server error.\n']

    def handle_request(self, req):
        """
        Entry point for api server.
        Should return a WSGI-style callable (such as swob.Response).

        :param req: swob.Request object
        """
        try:
            self.logger.set_statsd_prefix('api-server')
            if req.content_length and req.content_length < 0:
                self.logger.increment('errors')
                return HTTPBadRequest(request=req,
                                      body='Invalid Content-Length')

            try:
                if not check_utf8(req.path_info):
                    self.logger.increment('errors')
                    return HTTPPreconditionFailed(
                        request=req, body='Invalid UTF8 or contains NULL')
            except UnicodeError:
                self.logger.increment('errors')
                return HTTPPreconditionFailed(
                    request=req, body='Invalid UTF8 or contains NULL')

            try:
                controller, path_parts = self.get_controller(req.path)
                p = req.path_info
                if isinstance(p, unicode):
                    p = p.encode('utf-8')
            except ValueError:
                self.logger.increment('errors')
                return HTTPNotFound(request=req)
            if not controller:
                self.logger.increment('errors')
                return HTTPPreconditionFailed(request=req, body='Bad URL')
            if self.deny_host_headers and \
                    req.host.split(':')[0] in self.deny_host_headers:
                return HTTPForbidden(request=req, body='Invalid host header')

            self.logger.set_statsd_prefix('api-server.' +
                                          controller.server_type.lower())
            controller = controller(self, **path_parts)
            if 'gate.trans_id' not in req.environ:
                # if this wasn't set by an earlier middleware, set it now
                trans_id = 'tx' + uuid.uuid4().hex
                req.environ['gate.trans_id'] = trans_id
                self.logger.txn_id = trans_id
            req.headers['x-trans-id'] = req.environ['gate.trans_id']
            controller.trans_id = req.environ['gate.trans_id']
            self.logger.client_ip = get_remote_client(req)
            try:
                handler = getattr(controller, req.method)
                getattr(handler, 'publicly_accessible')
            except AttributeError:
                allowed_methods = getattr(controller, 'allowed_methods', set())
                return HTTPMethodNotAllowed(
                    request=req, headers={'Allow': ', '.join(allowed_methods)})
            if path_parts['version']:
                req.path_info_pop()
            if 'gate.authorize' in req.environ:
                # We call authorize before the handler, always. If authorized,
                # we remove the gate.authorize hook so isn't ever called
                # again. If not authorized, we return the denial unless the
                # controller's method indicates it'd like to gather more
                # information and try again later.
                resp = req.environ['gate.authorize'](req)
                if not resp:
                    # No resp means authorized, no delayed recheck required.
                    del req.environ['gate.authorize']
                else:
                    # Response indicates denial, but we might delay the denial
                    # and recheck later. If not delayed, return the error now.
                    if not getattr(handler, 'delay_denial', None):
                        return resp
            # Save off original request method (GET, POST, etc.) in case it
            # gets mutated during handling.  This way logging can display the
            # method the client actually sent.
            req.environ['gate.orig_req_method'] = req.method
            return handler(req)
        except (Exception, Timeout):
            self.logger.exception(_('ERROR Unhandled exception in request'))
            return HTTPServerError(request=req)

def app_factory(global_conf, **local_conf):
    """paste.deploy app factory for creating WSGI proxy apps."""
    conf = global_conf.copy()
    conf.update(local_conf)
    return Application(conf)