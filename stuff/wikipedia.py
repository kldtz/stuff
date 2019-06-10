import argparse
import re

import redis
from stuff.wiki_parser import iterate_pages

LINK = re.compile(r'\[\[(?:([^:\]|]+)?:)?([^:\]|#]+)(?:#([^|\]]+))?\]\]')


def extract_links(wiki_dump):
    for title, text in iterate_pages(wiki_dump):
        links = re.findall(LINK, text)
        filtered_links = [title for (prefix, title, anchor) in links if not prefix]
        yield title, filtered_links


def write_to_redis(r, title, links):
    if not links:
        return False
    if r.get(title):
        return False
    return r.rpush(title, *links)


def write_wiki_to_redis(r, wiki_dump):
    for title, links in extract_links(wiki_dump):
        write_to_redis(r, title, links)


def get_values(r, key):
    return set(r.lrange(key, 0, -1))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('wiki_dump', help='Path to Wikipedia dump')
    parser.add_argument('--host', help='Redis host', default='localhost')
    parser.add_argument('--port', help='Redis port', default=6379)
    args = parser.parse_args()

    r = redis.StrictRedis(host=args.host, port=args.port)
    write_wiki_to_redis(r, parser.wiki_dump)


if __name__ == '__main__':
    main()
