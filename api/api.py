from fastapi import FastAPI, Request
import uvicorn
from pydantic import BaseModel


class Item(BaseModel):
    name: str


async def items():
    return [1, 2, 3, 4]


app = FastAPI()


@app.post('/')
async def get_data(request: Request):
    data = await request.json()
    return data


@app.get('/')
async def get_items():
    data = await items()
    return data


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=5000)
