import argparse
import os
import re

import lucene
from java.lang import Float
from java.nio.file import Paths
from java.util import HashMap
from org.apache.lucene.document import DoublePoint, IntPoint
from org.apache.lucene.index import DirectoryReader, Term
from org.apache.lucene.queryparser.classic import MultiFieldQueryParser, QueryParser
from org.apache.lucene.search import (
    BooleanClause, BooleanQuery, BoostQuery, IndexSearcher, MatchAllDocsQuery, TermQuery,
)
from org.apache.lucene.store import FSDirectory

from analyzers import build_analyzer

lucene.initVM()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INDEX_DIR = os.path.join(BASE_DIR, 'index')

FIELD_BOOSTS = {
    'title': 3.0,
    'authors': 2.0,
    'subjects': 1.0,
    'categories': 1.0,
    'abstract': 1.0,
    'plot': 1.0,
}
TITLE_PHRASE_BOOST = 5.0
PLAIN_WORDS_RE = re.compile(r"[\w' ]+")
OPERATORS = {'AND', 'OR', 'NOT'}

MAX_TEXT = 800

MUST, SHOULD, FILTER = BooleanClause.Occur.MUST, BooleanClause.Occur.SHOULD, BooleanClause.Occur.FILTER


def date_bound(value: str, end: bool) -> int:
    if len(value) == 4:
        return int(value + ('1231' if end else '0101'))
    return int(value.replace('-', ''))


def words_query(text: str, analyzer):
    fields = list(FIELD_BOOSTS)
    boosts = HashMap()
    for field, boost in FIELD_BOOSTS.items():
        boosts.put(field, Float.valueOf(boost))
    parser = MultiFieldQueryParser(fields, analyzer, boosts)
    return QueryParser.cast_(parser).parse(text)


def title_phrase_query(text: str, analyzer):
    if not PLAIN_WORDS_RE.fullmatch(text) or OPERATORS & set(text.split()):
        return None
    query = QueryParser('title', analyzer).parse(f'"{text}"')
    return BoostQuery(query, TITLE_PHRASE_BOOST) if query is not None else None


def build_query(text, analyzer, language=None, subject=None, date_from=None, date_to=None,
                level_min=None, level_max=None):
    builder = BooleanQuery.Builder()

    if text.strip():
        builder.add(words_query(text, analyzer), MUST)
        phrase = title_phrase_query(text, analyzer)
        if phrase is not None:
            builder.add(phrase, SHOULD)
    else:
        builder.add(MatchAllDocsQuery(), MUST)

    if language:
        builder.add(TermQuery(Term('language', language.lower())), FILTER)
    if subject:
        builder.add(TermQuery(Term('subjects_exact', subject.lower())), FILTER)

    if date_from or date_to:
        a = date_bound(date_from, end=False) if date_from else 0
        b = date_bound(date_to, end=True) if date_to else 99991231
        builder.add(IntPoint.newRangeQuery('published_from', 0, b), FILTER)
        builder.add(IntPoint.newRangeQuery('published_to', a, 99991231), FILTER)

    if level_min is not None or level_max is not None:
        lo = level_min if level_min is not None else float('-inf')
        hi = level_max if level_max is not None else float('inf')
        builder.add(DoublePoint.newRangeQuery('reading_level', lo, hi), FILTER)

    return builder.build()


def print_hit(rank: int, score: float, doc) -> None:
    fields = {}
    for field in doc.getFields():
        value = field.stringValue()
        if value is None:
            value = str(field.numericValue())
        fields.setdefault(field.name(), []).append(value)

    print(f'{rank:2d}. [{score:6.2f}] {" | ".join(fields.pop("title", ["(no title)"]))}')
    for name, values in fields.items():
        text = ' | '.join(values)
        if len(text) > MAX_TEXT:
            text = text[:MAX_TEXT] + ' ...'
        print(f'      {name}: {text}')


def main():
    parser = argparse.ArgumentParser(
        description='Search the book index. Query syntax: word, "a phrase", AND, OR, NOT, ( ).')
    parser.add_argument('words', nargs='*', help='the query, e.g. \'(dragon OR wizard) AND "young boy" NOT harry\'')
    parser.add_argument('--language', help='filter, e.g. english')
    parser.add_argument('--subject', help='filter, exact subject, e.g. "science fiction"')
    parser.add_argument('--from', dest='date_from', help='filter, published from: 1990 or 1990-05-14')
    parser.add_argument('--to', dest='date_to', help='filter, published to: 1999 or 1999-12-31')
    parser.add_argument('--level-min', type=float, help='filter, minimal reading level')
    parser.add_argument('--level-max', type=float, help='filter, maximal reading level')
    parser.add_argument('--top', type=int, default=10, help='how many results to show')
    args = parser.parse_args()

    analyzer = build_analyzer()
    reader = DirectoryReader.open(FSDirectory.open(Paths.get(INDEX_DIR)))
    try:
        searcher = IndexSearcher(reader)
        try:
            query = build_query(
                ' '.join(args.words), analyzer, args.language, args.subject,
                args.date_from, args.date_to, args.level_min, args.level_max,
            )
        except lucene.JavaError as error:
            print('wrong query:', error.getJavaException().getMessage())
            return
        print('query:', query)

        top_docs = searcher.search(query, args.top)
        total = top_docs.totalHits.value
        total = total() if callable(total) else total
        print(f'found: {total}\n')

        stored = searcher.storedFields()
        for rank, hit in enumerate(top_docs.scoreDocs, start=1):
            print_hit(rank, hit.score, stored.document(hit.doc))
    finally:
        reader.close()


if __name__ == '__main__':
    main()
