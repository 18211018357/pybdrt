# -*- coding: utf-8 -*-

from __future__ import absolute_import

from pybdrt.cursor import Cursor
from pybdrt.avatica.errors import ProgrammingError, InternalError
from .proxy import Proxy
from .log import logger
import uuid
import pprint
import requests
from pybdrt import errors
from pybdrt.avatica.proto import requests_pb2, common_pb2, responses_pb2
import httplib
import urllib2
import math
import time
import random
import sys

class Connection(object):

    def __init__(self, username, password, url, database, **kwargs):
        self.url = url
        self.username = username
        self.password = password
        self._id =  '%s' % uuid.uuid4()
        self.connection_id = '%s' % uuid.uuid4()
        self.max_retries = 3
        self.database = database
        self.proxy = Proxy(self.url)
        self.limit = kwargs['limit'] if 'limit' in kwargs else 50000

        self.proxy.login(self.username, self.password, self.connection_id, self.database)

    def close(self):

        if self._closed:
            raise ProgrammingError('the connection is already closed')
        for cursor_ref in self._cursors:
            cursor = cursor_ref()
            if cursor is not None and not cursor._closed:
                cursor.close()
        self._client.close_connection(self._id)
        self._client.close()
        self._closed = True

        logger.debug('Connection close called')

    def commit(self):
        logger.warn('Transactional commit is not supported')

    def rollback(self):
        logger.warn('Transactional rollback is not supported')

    def list_tables(self):
        route = 'tables_and_columns'
        params = {'project': self.project}
        tables = self.proxy.get(route, params=params)
        return [t['table_NAME'] for t in tables]

    def list_columns(self, table_name):
        table_NAME = str(table_name).upper()
        route = 'tables_and_columns'
        params = {'project': self.project}
        tables = self.proxy.get(route, params=params)
        table = [t for t in tables if t['table_NAME'] == table_NAME][0]
        return table['columns']

    def cursor(self):
        return Cursor(self)

    def prepare_and_execute(self, connection_id, statement_id, sql, max_rows_total=None, first_frame_max_size=None):
        """Prepares and immediately executes a statement.

        :param connection_id:
            ID of the current connection.

        :param statement_id:
            ID of the statement to prepare.

        :param sql:
            SQL query.

        :param max_rows_total:
            The maximum number of rows that will be allowed for this query.

        :param first_frame_max_size:
            The maximum number of rows that will be returned in the first Frame returned for this query.

        :returns:
            Result set with the signature of the prepared statement and the first frame data.
        """
        request = requests_pb2.PrepareAndExecuteRequest()
        request.connection_id = connection_id
        request.statement_id = statement_id
        request.sql = sql
        request.max_row_count=10
        request.max_rows_total=10
        request.first_frame_max_size=10

        '''
        if max_rows_total is not None:
            request.max_rows_total = max_rows_total
        if first_frame_max_size is not None:
            request.first_frame_max_size = first_frame_max_size
        '''
        response_data = self._apply(request, 'ExecuteResponse')
        response = responses_pb2.ExecuteResponse()
        response.ParseFromString(response_data)
        return response.results


    def _apply(self, request, expected_response_type=None):
        logger.debug("Sending request\n%s", pprint.pformat(request))

        request_name = request.__class__.__name__

        message = common_pb2.WireMessage()
        message.name = 'org.apache.calcite.avatica.proto.Requests${}'.format(request_name)
        message.wrapped_message = request.SerializeToString()

        body = message.SerializeToString()
        headers = {'content-type': 'application/octet-stream'}

        urllib__request = urllib2.Request(self.url, data=body, headers=headers)

        urlopen = urllib2.urlopen(urllib__request)

        urlopen_read = urlopen.read()

        print urlopen_read

        requests_request = requests.request("POST", self.url, data=body, headers=headers)

        response = self._post_request(body, headers)
        response_body = response.read()

        if response.status != 200:
            logger.debug("Received response\n%s", response_body)
            raise errors.InterfaceError('RPC request returned invalid status code', response.status)

        message = common_pb2.WireMessage()
        message.ParseFromString(response_body)

        logger.debug("Received response\n%s", message)

        if expected_response_type is None:
            expected_response_type = request_name.replace('Request', 'Response')

        expected_response_type = 'org.apache.calcite.avatica.proto.Responses$' + expected_response_type
        if message.name != expected_response_type:
            raise errors.InterfaceError('unexpected response type "{}"'.format(message.name))

        return message.wrapped_message

    def _post_request(self, body, headers):
        retry_count = self.max_retries
        while True:
            logger.debug("POST %s %r %r", self.url, body, headers)
            try:
                #self.url = "http://192.168.253.1:1234"
                resp = requests.post(self.url, data=body, headers=headers)
                #print resp

                #self.connection.request('POST', url=self.url, body=body, headers=headers)
                #response = self.connection.getresponse()

            except httplib.HTTPException as e:
                '''
                if retry_count > 0:
                    delay = math.exp(-retry_count)
                    logger.debug("HTTP protocol error, will retry in %s seconds...", delay, exc_info=True)
                    self.close()
                    self.connect()
                    time.sleep(delay)
                    retry_count -= 1
                    continue
                raise errors.InterfaceError('RPC request failed', cause=e)
                '''
            else:
                '''
                if resp.status == httplib.SERVICE_UNAVAILABLE:
                    if retry_count > 0:
                        delay = math.exp(-retry_count)
                        logger.debug("Service unavailable, will retry in %s seconds...", delay, exc_info=True)
                        time.sleep(delay)
                        retry_count -= 1
                        continue
                '''
                return resp

    def prepare(self, connection_id, sql, max_rows_total=None):
        """Prepares a statement.

        :param connection_id:
            ID of the current connection.

        :param sql:
            SQL query.

        :param max_rows_total:
            The maximum number of rows that will be allowed for this query.

        :returns:
            Signature of the prepared statement.
        """
        request = requests_pb2.PrepareRequest()
        request.connection_id = connection_id
        request.sql = sql
        if max_rows_total is not None:
            request.max_rows_total = max_rows_total

        response_data = self._apply(request)
        response = responses_pb2.PrepareResponse()
        response.ParseFromString(response_data)
        return response.statement

def connect(username='', password='', url='', database='', **kwargs):
    return Connection(username, password, url, database, **kwargs)

