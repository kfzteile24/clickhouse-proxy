from clickhouse_proxy.config import config
from clickhouse_proxy.fsm import FSM

import logging
import os
import re

import requests
import starlette.responses as resp
import uvicorn
from starlette.applications import Starlette

app = Starlette(debug=True)

# sqlparse can't understand ".... FORMAT whatever;" syntax, so let's just take care of it.
rg = re.compile(r"FORMAT\s+[a-zA-Z]+;$", re.IGNORECASE)

logpath = os.path.dirname(config.log_file)
if not os.path.exists(logpath):
    os.makedirs(logpath)
logging.basicConfig(
    filename=config.log_file,
    level=getattr(logging, config.log_level.upper()),
    format='%(asctime)s : %(levelname)s : %(message)s'
)
reqlog = logging.getLogger(__name__)


@app.route('/', methods=['GET', 'POST'])
async def homepage(request):
    url = request.url
    url_base = f"{url.scheme}://{url.netloc}"
    s_url = str(url)
    new_url = f"{config.clickhouse_scheme}://{config.clickhouse_host}:{config.clickhouse_port}{s_url[len(url_base):]}"

    reqlog.info("incoming request: " + s_url)
    body = await request.body()
    reqlog.debug("body:\n\n")
    reqlog.debug(body)
    headers = dict(request.headers)
    headers['host'] = f'{config.clickhouse_host}:{config.clickhouse_port}'
    headers.pop('content-length', None)
    reqlog.debug("overwrite url: " + new_url)

    if body:
        strbody = body.decode(config.encoding)
        fsm = FSM()

        # sqlparse can't understand ".... FORMAT whatever;" syntax, so let's just take care of it.
        # TODO: Get rid of sqlparse and use a proper state machine without regex. Weigh the benefits of making it a flat
        # structure
        fmtmatch = rg.search(strbody)
        fmtstr = ''
        if fmtmatch:
            fmtstr = strbody[fmtmatch.start():]
            strbody = strbody[:fmtmatch.start()]
        strbody = fsm.replace_odbc_tokens(strbody)
        strbody = fsm.replace_paranoid_joins(strbody)
        body = (strbody + fmtstr).encode(config.encoding)

        reqlog.debug("overwrite body:\n\n")
        reqlog.debug(strbody + fmtstr)

    response = requests.request(request.method, new_url, headers=headers, data=body)

    if response.headers['Content-Type'][:5] == 'text/':
        return resp.Response(response.content, response.status_code, response.headers)


def main():
    uvicorn.run(app, host=config.listen_host, port=config.listen_port)


if __name__ == '__main__':
    main()
