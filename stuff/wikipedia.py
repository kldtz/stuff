import argparse
import bz2
import re
import xml.etree.ElementTree as ET

import redis

DEFAULT_NAMESPACE = '{http://www.mediawiki.org/xml/export-0.10/}'
PAGE = DEFAULT_NAMESPACE + 'page'
TITLE = DEFAULT_NAMESPACE + 'title'
TEXT = DEFAULT_NAMESPACE + 'text'

LINK = re.compile(r'\[\[(?:([^:\]|]+)?:)?([^:\]|#]+)(?:#([^|\]]+))?\]\]')


def iterate_pages(wiki_dump):
    title, text = '', ''
    with bz2.open(wiki_dump, 'r') as dump:
        for event, elem in ET.iterparse(dump, events=("start", "end")):
            if event == "start" and elem.tag == PAGE:
                title, text = '', ''
            if event == "start" and elem.tag == TITLE:
                title = elem.text
            if event == "end" and elem.tag == TEXT:
                text = "".join(elem.itertext())
            if event == "end" and elem.tag == PAGE:
                if title and text:
                    yield title, text
            elem.clear()


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
    wiki_dump = '/home/tobias/data/wiki/dewiki-latest-pages-articles.xml.bz2'
    r = redis.StrictRedis(host='localhost', port=6379)
    # write_wiki_to_redis(r, wiki_dump)
    leipzig = get_values(r, 'Leipzig')
    print(leipzig.intersection(get_values(r, 'Berlin')))
