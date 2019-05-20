import logging
import os
import re

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
        if config.log_level.upper() == 'DEBUG':
            self.__fl = FileLogger(config.log_dir_debug)
        else:
            self.__fl = DummyLogger()


    def __call__(self, req, resp):
        print(dir(req))
        self.__num += 1
        call_id = f'{id(self)}-{self.__num}'
        reqlog.debug(f"Init request ID: {call_id}")
        self.__fl.begin(call_id)
        self.__fl.log('request0', str(req)[1:-1])
        headers = dict(req.headers)
        self.__fl.log('request0', '\n'.join([f'{k}: {v}' for k, v in headers.items()]))
        url_base = f"{req.scheme}://{req.netloc}"
        new_url = f"{config.clickhouse_scheme}://{config.clickhouse_host}:{config.clickhouse_port}{req.url[len(url_base):]}"

        body = req.bounded_stream.read()
        self.__fl.log('request0', '')
        self.__fl.log('request0', body)

        headers['HOST'] = f'{config.clickhouse_host}:{config.clickhouse_port}'
        headers.pop('CONTENT_LENGTH', None)
        reqlog.debug("overwrite url: " + new_url)

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

            reqlog.debug("overwrite body:\n\n")
            reqlog.debug(strbody + fmtstr)

        response = requests.request(req.method, new_url, headers=headers, data=body)

        reqlog.debug("Response content type:")

        resp.status = f'{response.status_code} {response.reason}'
        resp.content_type = response.headers['Content-Type']
        for (k, v) in response.headers.items():
            resp.headers[k.upper()] = v
        resp.body = response.content

        # More here: https://falcon.readthedocs.io/en/stable/user/quickstart.html


def main():
    app = falcon.API()
    app.add_sink(MainResource(), r'/.*')
    try:
        import uvicorn0
    except ModuleNotFoundError:
        from wsgiref import simple_server

    if 'uvicorn' in locals():
        print("Running in uvicorn")
        uvicorn.run(app, host=config.listen_host, port=config.listen_port)
    else:
        # Alternative if no uvicorn
        print("Running in simple_server")
        httpd = simple_server.make_server('0.0.0.0', 8000, app)
        httpd.serve_forever()


if __name__ == '__main__':
    main()
