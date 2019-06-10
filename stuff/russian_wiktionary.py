import argparse
import re

import redis
from stuff.wiki_parser import iterate_pages

LINK = re.compile(r'\[\[(?:([^:\]|]+)?:)?([^:\]|#]+)(?:#([^|\]]+))?\]\]')
RUSSIAN = re.compile(r'==\s*Russian\s*==')


def iterate_russian_pages(wiki_dump):
    for title, text in iterate_pages(wiki_dump):
        if has_cyrillic(title) and re.search(RUSSIAN, text):
            yield title, text


def has_cyrillic(text):
    return bool(re.search('[\u0400-\u04FF]', text))


def write_to_redis(r, title, text):
    if not text:
        return False
    if r.get(title):
        return False
    return r.set(title, text)


def write_russian_words(r, wiki_dump):
    for title, text in iterate_russian_pages(wiki_dump):
        write_to_redis(r, title, text)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--wiki_dump', help='Path to Wiktionary dump')
    parser.add_argument('--host', help='Redis host', default='localhost')
    parser.add_argument('--port', help='Redis port', default=6379)
    args = parser.parse_args()

    r = redis.StrictRedis(host=args.host, port=args.port, db=0)
    if args.wiki_dump:
        write_russian_words(r, args.wiki_dump)


if __name__ == '__main__':
    main()
