import asyncio
import requests


async def get_data():
    url = 'http://127.0.0.1:8000/'
    myobj = {'somekey': 'somevalue'}
    await asyncio.sleep(2)
    res = requests.post(url=url, json=myobj)
    return res


def get_res(data):
    return data


async def main():

    data = await get_data()
    e = get_res(data)


if __name__ == '__main__':
    asyncio.run(main())
