import logging
import os
import re
import sys

from typing import Dict, List, Tuple

from clickhouse_proxy.config import config
from clickhouse_proxy.file_logger import DummyLogger, FileLogger
from clickhouse_proxy.fsm import FSM
from clickhouse_proxy import auth

import requests

logpath = os.path.dirname(config.log_file)
if not os.path.exists(logpath):
    os.makedirs(logpath)
logging.basicConfig(
    filename=config.log_file,
    level=getattr(logging, config.log_level.upper()),
    format='%(asctime)s : %(levelname)s : %(message)s'
)
reqlog = logging.getLogger(__name__)


__fmt_rg = re.compile(r"FORMAT\s+[a-zA-Z]+;$", re.IGNORECASE)
__suffix_rg = re.compile(r"([\r\n]+[0][\r\n]+)+$")
__num = 0
# A detailed logger for each request, that puts each payload in a separate file
if config.log_level.upper() == 'DEBUG':
    __fl = FileLogger(config.log_dir_debug)
else:
    __fl = DummyLogger()


def __match_format_sql(query) -> Tuple[str, str]:
    # sqlparse can't understand ".... FORMAT whatever;" syntax, so let's just take care of it.
    # TODO: Get rid of sqlparse and use a proper state machine without regex. Weigh the benefits of making it a flat
    # structure
    fmtmatch = __fmt_rg.search(query)
    fmtstr = ''
    if fmtmatch:
        fmtstr = query[fmtmatch.start():]
        query = query[:fmtmatch.start()]
    return query, fmtstr


def __params_to_dict(param_str: str) -> Dict[str, str]:
    # Split everything
    params = (pv.split('=') for pv in param_str.split('&') if pv)
    # Organize into dicts with default value True
    return dict(([kv[0], kv[1] if len(kv)>1 else True] for kv in params))


def __headers_to_dict(headers: List[List[bytes]]) -> Dict[str, str]:
    return dict(([k.decode('utf-8'), v.decode('utf-8')] for k, v in headers))


def __dict_to_headers(dheaders: Dict[str, str]) -> List[List[bytes]]:
    return [[k.encode('utf-8'), v.encode('utf-8')] for k, v in dheaders.items()]


def __chunk_request(body):
    yield body


async def read_body(receive):
    body = b''
    more_body = True
    while more_body:
        message = await receive()
        body += message.get('body', b'')
        more_body = message.get('more_body', False)
    return body


async def app(scope, receive, send):
    if scope['type'] == 'lifespan':
        return

    global __num
    __num += 1
    call_id = f'{id(app)}-{__num:0>5}'
    params = ''
    # scope['query_string'] = b'a=b&c=d'
    if scope.get('query_string', b''):
        params = '?' + scope['query_string'].decode('utf-8')
    url_base = f"{scope['scheme']}://{scope['server'][0]}:{scope['server'][1]}"
    headers = __headers_to_dict(scope["headers"])

    reqlog.debug(f"Init request ID: {call_id}")
    __fl.begin(call_id)
    __fl.log('request0', f"Request: {scope['method']} '{url_base}{scope['path']}{params}'")
    __fl.log('request0', headers)

    # scope['path'] = '/'
    new_url = f"{config.clickhouse_scheme}://{config.clickhouse_host}:{config.clickhouse_port}{scope['path']}{params}"

    body = await read_body(receive)
    __fl.log('request0', '')
    __fl.log('request0', body[:config.log_length_debug])

    # scope['client'] = ('127.0.0.1', 49634)
    auth_result = auth.authorize(__params_to_dict(params), scope['client'][0])
    if auth_result:
        status = '403 Not Authorized'
        __fl.log('response', status)
        __fl.log('response', '')
        __fl.log('response', auth_result)
        res_headers = {'content-type': 'text/plain'}
        if headers.get('transfer-encoding', '') == 'chunked':
            res_headers['transfer-encoding'] = 'chunked'
        await send({
            'type': 'http.response.start',
            'status': 403,
            'headers': __dict_to_headers(res_headers)
        })
        await send({
            'type': 'http.response.body',
            'body': auth_result.encode('utf-8')
        })
        return

    headers['host'] = f'{config.clickhouse_host}:{config.clickhouse_port}'
    headers.pop('content_length', None)

    if body:
        strbody = body.decode(config.encoding)

        # [....] FORMAT whatever;
        # from the end of the SQL
        strbody, fmtstr = __match_format_sql(strbody)

        fsm = FSM()
        strbody = fsm.replace_odbc_tokens(strbody)
        strbody = fsm.replace_paranoid_joins(strbody)
        body = (strbody + fmtstr).encode(config.encoding)

    # headers['content_length'] = len(body)
    __fl.log('request1', f"Request: {scope['method']} '{new_url}'")
    __fl.log('request1', headers)
    __fl.log('request1', '')
    __fl.log('request1', body[:config.log_length_debug])

    __fl.log('response', "Response:")

    body_provider = None
    if headers.get('transfer-encoding', '') == 'chunked':
        body_provider = __chunk_request(body)
    else:
        body_provider = body

    response = requests.request(scope['method'], new_url, headers=headers, data=body_provider)

    status = f'{response.status_code} {response.reason}'
    res_headers = dict(response.headers)

    __fl.log('response', status)
    __fl.log('response', res_headers)
    __fl.log('response', '')
    __fl.log('response', response.content[:config.log_length_debug])

    await send({
        'type': 'http.response.start',
        'status': response.status_code,
        'headers': __dict_to_headers(response.headers)
    })
    await send({
        'type': 'http.response.body',
        'body': response.content
    })


def main():
    import uvicorn
    uvicorn.run(app, host=config.listen_host, port=config.listen_port)


def test():
    assert __params_to_dict('a=b&c=d&e') == {'a': 'b', 'c': 'd', 'e': True}
    assert __params_to_dict('&') == {}
    assert __params_to_dict('') == {}


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'test':
        test()
    else:
        main()
