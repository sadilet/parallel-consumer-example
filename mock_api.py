import uvicorn
from starlette.applications import Starlette
from starlette.responses import JSONResponse


app = Starlette()


@app.route('/api')
async def get_json(request):
    return JSONResponse({'hello': 'world'})


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8080)