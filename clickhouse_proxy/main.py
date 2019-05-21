import logging
import os
import re
import socket
import struct

from clickhouse_proxy.config import config
from clickhouse_proxy.file_logger import DummyLogger, FileLogger
from clickhouse_proxy.fsm import FSM

import requests
import falcon


logpath = os.path.dirname(config.log_file)
if not os.path.exists(logpath):
    os.makedirs(logpath)
logging.basicConfig(
    filename=config.log_file,
    level=getattr(logging, config.log_level.upper()),
    format='%(asctime)s : %(levelname)s : %(message)s'
)
reqlog = logging.getLogger(__name__)


class MainResource(object):
    def __init__(self):
        # sqlparse can't understand ".... FORMAT whatever;" syntax, so let's just take care of it.
        self.__rg = re.compile(r"FORMAT\s+[a-zA-Z]+;$", re.IGNORECASE)
        self.__num = 0
        # A detailed logger for each request, that puts each payload in a separate file
        if config.log_level.upper() == 'DEBUG':
            self.__fl = FileLogger(config.log_dir_debug)
        else:
            self.__fl = DummyLogger()

    def __call__(self, req, resp = None):
        self.__num += 1
        call_id = f'{id(self)}-{self.__num}'
        reqlog.debug(f"Init request ID: {call_id}")
        self.__fl.begin(call_id)
        self.__fl.log('request0', str(req)[1:-1])
        headers = dict(req.headers)
        self.__fl.log('request0', dict(headers))
        url_base = f"{req.scheme}://{req.netloc}"
        new_url = f"{config.clickhouse_scheme}://{config.clickhouse_host}:{config.clickhouse_port}{req.url[len(url_base):]}"

        body = req.bounded_stream.read()
        self.__fl.log('request0', '')
        self.__fl.log('request0', body)

        auth_result = self.__authorise(req.params, req.remote_addr)
        if auth_result is not None:
            status = '403 Not Authorized'
            self.__fl.log('response', status)
            self.__fl.log('response', dict(resp.headers))
            self.__fl.log('response', '')
            self.__fl.log('response', auth_result)
            resp.status = status
            resp.body = auth_result
            return

        headers['HOST'] = f'{config.clickhouse_host}:{config.clickhouse_port}'
        headers.pop('CONTENT_LENGTH', None)

        self.__fl.log('request1', f"Request: {req.method} '{new_url}'")
        self.__fl.log('request1', dict(headers))

        if body:
            strbody = body.decode(config.encoding)
            fsm = FSM()

            # sqlparse can't understand ".... FORMAT whatever;" syntax, so let's just take care of it.
            # TODO: Get rid of sqlparse and use a proper state machine without regex. Weigh the benefits of making it a flat
            # structure
            fmtmatch = self.__rg.search(strbody)
            fmtstr = ''
            if fmtmatch:
                fmtstr = strbody[fmtmatch.start():]
                strbody = strbody[:fmtmatch.start()]
            strbody = fsm.replace_odbc_tokens(strbody)
            strbody = fsm.replace_paranoid_joins(strbody)
            body = (strbody + fmtstr).encode(config.encoding)

            self.__fl.log('request1', '')
            self.__fl.log('request1', body)

        response = requests.request(req.method, new_url, headers=headers, data=body)

        resp.status = f'{response.status_code} {response.reason}'
        self.__fl.log('response', resp.status)
        self.__fl.log('response', dict(response.headers))
        self.__fl.log('response', '')
        self.__fl.log('response', response.content)
        resp.content_type = response.headers['Content-Type']
        for (k, v) in response.headers.items():
            resp.headers[k.upper()] = v
        resp.body = response.content

        # More here: https://falcon.readthedocs.io/en/stable/user/quickstart.html


app = falcon.API()
app.add_sink(MainResource(), r'/.*')

if __name__=='__main__':
    from wsgiref import simple_server
    httpd = simple_server.make_server(config.listen_host, config.listen_port, app)
    httpd.serve_forever()
