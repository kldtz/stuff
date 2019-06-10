import bz2
import xml.etree.ElementTree as ET

PAGE = 'page'
TITLE = 'title'
TEXT = 'text'
MEDIAWIKI = 'mediawiki'


def iterate_pages(wiki_dump):
    title, text = '', ''
    root = None
    with bz2.open(wiki_dump, 'r') as dump:
        for event, elem in ET.iterparse(dump, events=("start", "end")):
            tag = strip_ns(elem.tag)
            if event == 'start':
                if tag == MEDIAWIKI:
                    root = elem
                if tag == PAGE:
                    title, text = '', ''
            if event == 'end':
                if tag == TITLE and not title:  # first title under page
                    title = ''.join(elem.itertext())
                if tag == TEXT and not text:  # first text under page
                    text = ''.join(elem.itertext())
                if tag == PAGE:
                    if title and text:
                        yield title, text
                    root.clear()
        root.clear()


def strip_ns(tag):
    ns_end = tag.rfind('}')
    if ns_end != -1:
        return tag[ns_end + 1:]
    return tag
