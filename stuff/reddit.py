import argparse
import asyncio
import json

import aiohttp

REDDIT_URL = 'https://www.reddit.com'


async def get_json(client, url):
    async with client.get(url) as response:
        assert response.status == 200
        return await response.read()


async def get_reddit_top(subreddit, client, limit):
    dat = await get_json(client, f'{REDDIT_URL}/r/{subreddit}/top.json?sort=top&t=day&limit={limit}')
    score_sum = 0
    json_dict = json.loads(dat.decode('utf-8'))
    print(f'=== {subreddit.title()} ===')
    for child in json_dict['data']['children']:
        score = child['data']['score']
        title = child['data']['title']
        link = child['data']['url']
        permalink = child['data']['permalink']
        print(f"{score:>6}: {title}")
        print(f"{'':>8}  Discussion: {REDDIT_URL + permalink}")
        if permalink not in link:
            print(f"{'':>8}  Link: {'':>6}{link}")
        score_sum += score
    return score_sum


async def get_all_reddit_tops(topics, limit=5):
    async with aiohttp.ClientSession() as session:
        tasks = [asyncio.ensure_future(get_reddit_top(topic, session, limit)) for topic in topics]
        responses = await asyncio.gather(*tasks)
        print('------')
        print('{:>6}'.format(sum(responses)))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('topics', nargs='*', default=['coding', 'compsci', 'python', 'programming'])
    parser.add_argument('-n', default=5, type=int, dest='n')
    args = parser.parse_args()

    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(get_all_reddit_tops(args.topics, args.n))
    loop.run_until_complete(future)


if __name__ == '__main__':
    main()
