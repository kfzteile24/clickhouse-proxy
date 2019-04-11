from starlette.applications import Starlette
from starlette.responses import JSONResponse
import starlette.responses as resp
import requests
import uvicorn

app = Starlette(debug=True)

@app.route('/', methods=['GET', 'POST'])
async def homepage(request):
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
    response = requests.request(request.method, str(request.url).replace('localhost:8000', 'localhost:8124'),
        headers=headers,
        data=body)
    print('----------------------------')
    print(response.content, response.headers, dir(response))
    if response.headers['Content-Type'][:5] == 'text/':
        return resp.Response(response.content, response.status_code, response.headers)
    

if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)
