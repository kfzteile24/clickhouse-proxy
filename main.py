from starlette.applications import Starlette
from starlette.responses import JSONResponse
import starlette.responses as resp
import requests
import uvicorn
from fsm import FSM
import re

app = Starlette(debug=True)

# sqlparse can't understand ".... FORMAT whatever;" syntax, so let's just take care of it.
rg = re.compile("FORMAT\s+[a-zA-Z]+;$", re.IGNORECASE)

@app.route('/', methods=['GET', 'POST'])
async def homepage(request):
    print("============ BEGIN REQUEST =============")
    print("========================================")
    print('headers: ', request.headers)
    #print('auth: ', request.auth)
    body = await request.body()
    print('body: ', body)
    print('query_params: ', request.query_params)
    print('method: ', request.method)
    print('path_params: ', request.path_params)
    print('url: ', request.url, request.url.username, request.url.password)

    headers = dict(request.headers)
    headers['host'] = 'localhost:8124'
    headers.pop('content-length', None)
    url = str(request.url).replace('localhost:8000', 'localhost:8124')
    print('++++++++++++++++++++++++++++')
    print(url, headers)

    strbody = body.decode('utf-8')
    fsm = FSM()
    # sqlparse can't understand ".... FORMAT whatever;" syntax, so let's just take care of it.
    fmtmatch = rg.search(strbody)
    fmtstr = ''
    if fmtmatch:
        fmtstr = strbody[fmtmatch.start():]
        strbody = strbody[:fmtmatch.start()]
    strbody = fsm.replace_odbc_tokens(strbody)
    strbody = fsm.replace_paranoid_joins(strbody) + fmtstr
    body = strbody.encode('utf-8')

    print('\n', '\n', strbody, '\n', '\n')

    response = requests.request(request.method, str(request.url).replace('localhost:8000', 'localhost:8124'),
        headers=headers,
        data=body)
    print('----------------------------')
    print(response.content, response.headers, dir(response))
    if response.headers['Content-Type'][:5] == 'text/':
        return resp.Response(response.content, response.status_code, response.headers)


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)
