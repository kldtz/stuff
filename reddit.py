import asyncio
import json

import aiohttp


async def get_json(client, url):
    async with client.get(url) as response:
        assert response.status == 200
        return await response.read()


async def get_reddit_top(subreddit, client, limit):
    dat = await get_json(client, f'https://www.reddit.com/r/{subreddit}/top.json?sort=top&t=day&limit={limit}')
    score_sum = 0
    json_dict = json.loads(dat.decode('utf-8'))
    print(subreddit.title())
    for child in json_dict['data']['children']:
        score = child['data']['score']
        title = child['data']['title']
        link = child['data']['url']
        print("{:>6}: {} ({})".format(score, title, link))
        score_sum += score
    return score_sum


async def get_all_reddit_tops(topics, limit=5):
    async with aiohttp.ClientSession() as session:
        tasks = [asyncio.ensure_future(get_reddit_top(topic, session, limit)) for topic in topics]
        responses = await asyncio.gather(*tasks)
        print('------')
        print('{:>6}'.format(sum(responses)))


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(get_all_reddit_tops(['coding', 'compsci', 'python', 'programming']))
    loop.run_until_complete(future)
