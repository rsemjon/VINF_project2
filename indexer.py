import json
import os

import lucene
from java.nio.file import Paths
from org.apache.lucene.document import Document, DoublePoint, Field, IntPoint, StoredField, StringField, TextField
from org.apache.lucene.index import IndexWriter, IndexWriterConfig
from org.apache.lucene.store import FSDirectory

from analyzers import build_analyzer

lucene.initVM()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_JSONL = os.path.join(BASE_DIR, 'data', 'wiki_join_craw_attempt4.jsonl')
INDEX_DIR = os.path.join(BASE_DIR, 'index')

STORE = Field.Store.YES


def date_int(iso: str) -> int:
    return int(iso.replace('-', ''))


def add_text(doc, name: str, values) -> None:
    for value in values if isinstance(values, list) else [values]:
        if value:
            doc.add(TextField(name, value, STORE))


def to_document(book: dict):
    doc = Document()

    doc.add(StringField('id', str(book['id']), STORE))

    add_text(doc, 'title', book['title'])
    add_text(doc, 'authors', book['authors'])
    add_text(doc, 'subjects', book['subjects'])
    add_text(doc, 'categories', book['categories'])
    add_text(doc, 'abstract', book['abstract'])
    add_text(doc, 'plot', book['plot'])

    for subject in book['subjects'] or []:
        doc.add(StringField('subjects_exact', subject.lower(), Field.Store.NO))

    if book['language']:
        doc.add(StringField('language', book['language'].lower(), Field.Store.NO))
        doc.add(StoredField('language', book['language']))

    if book['reading_level'] is not None:
        doc.add(DoublePoint('reading_level', float(book['reading_level'])))
        doc.add(StoredField('reading_level', float(book['reading_level'])))

    if book['published_from'] and book['published_to']:
        doc.add(IntPoint('published_from', date_int(book['published_from'])))
        doc.add(IntPoint('published_to', date_int(book['published_to'])))
        doc.add(StoredField('published_from', book['published_from']))
        doc.add(StoredField('published_to', book['published_to']))

    if book['published']:
        doc.add(StoredField('published', book['published']))

    return doc


def main():
    config = IndexWriterConfig(build_analyzer())
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE)
    writer = IndexWriter(FSDirectory.open(Paths.get(INDEX_DIR)), config)

    count = 0
    try:
        with open(INPUT_JSONL, encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    writer.addDocument(to_document(json.loads(line)))
                    count += 1

        writer.commit()
    finally:
        writer.close()

    print(f'indexed {count} books into {INDEX_DIR}')


if __name__ == '__main__':
    main()
