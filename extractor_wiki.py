import glob
import os
import shutil

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType


SPARK_REMOTE = "sc://localhost:15002"

CONTAINER_INPUT_DIR = "/data"
CONTAINER_OUTPUT_DIR = "/output"

BOOKS_CSV = f'{CONTAINER_OUTPUT_DIR}/books_extracted/books_wiki.csv'
WIKI_DUMPS = [
    f'{CONTAINER_OUTPUT_DIR}/wiki/enwiki-latest-pages-articles.xml.bz2',
    # f'{CONTAINER_OUTPUT_DIR}/wiki/wiki-sample3.bz2',
    # f'{CONTAINER_OUTPUT_DIR}/wiki/enwiki-latest-pages-articles11.xml-p5399367p6899366.bz2',
    # f'{CONTAINER_OUTPUT_DIR}/wiki/enwiki-latest-pages-articles11.xml-p6899367p7054859.bz2',
    # f'{CONTAINER_OUTPUT_DIR}/wiki/enwiki-latest-pages-articles14.xml-p13159683p14324602.bz2',
    # f'{CONTAINER_OUTPUT_DIR}/wiki/enwiki-latest-pages-articles15.xml-p14324603p15824602.bz2',
]

HOST_OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
JOIN_JSON = 'wiki_join_craw_attempt4.jsonl'

MAX_PARTITION_BYTES = 16 * 1024 * 1024
MIN_PARTITIONS = 18

BOOK_INFOBOX_RE = r'(?i)\{\{\s*infobox[ _]+(?:book|novel|short[ _]+story|poem)\s*[|\n}]'

BOOK_TYPES = r'books|novels|novellas|poetry collections|short story collections'
BOOK_CATEGORY_EXCLUDE = r'based on|adaptations? of|characters|lists? of|series'
BOOK_CATEGORY_RE = (
    r'(?i)\[\[\s*Category\s*:\s*'
    rf'(?![^\]|]*\b(?:{BOOK_CATEGORY_EXCLUDE})\b)'
    rf'([^\]|]*?\b(?:{BOOK_TYPES}))\s*(?:\|[^\]]*)?\]\]'
)

PLOT_RE = (
    r'(?ims)^(={2,})[^=\n]*\bplot\b[^=\n]*\1[ \t]*$\s*'
    r'(.*?)\s*(?=^\1[^=]|^==[^=]|</text>|\z)'
)

LINK_RE = r'(?i)\[\[(?:\s*(?:File|Image|Category)\s*:[^\[\]]*|(?:[^\[\]|]*\|)*([^\[\]|]*))\]\]'

TAG_BODY = r'[^&]*(?:&(?!gt;)[^&]*)*'
COMMENT_RE = r'&lt;!--.*?(?:--&gt;|\z)'
REF_RE = rf'&lt;ref\b{TAG_BODY}/&gt;|&lt;ref\b{TAG_BODY}(?<!/)&gt;.*?&lt;/ref\s*&gt;'
BR_RE = r'(?i)&lt;br\s*/?\s*&gt;'
SPACE_HTML_RE = rf'&lt;/?[a-zA-Z]{TAG_BODY}&gt;|&(?:amp;)*(?:nbsp|ensp|emsp|thinsp|ndash|mdash|minus);'
ENTITY_NAME = r'(?:[a-zA-Z]+\d*|#\d+|#[xX][0-9a-fA-F]+);'
ENTITY_RE = rf'(&)amp;(?!(?:amp;)*{ENTITY_NAME})|&(?:amp;)*{ENTITY_NAME}'

VALUE_NOISE_RE = (
    rf'(?is){COMMENT_RE}|{REF_RE}'
    r'|\{\{\s*(?:plainlist|flatlist|hlist|ubl|unbulleted[ _]list)\s*|\}\}'
)

NOT_IN_LINK = r'(?![^\[\]]*\]\])'
LIST_SEP_RE = rf'\s*(?:,(?!\s*[JS]r\b)|[;|\n])[\s*]*{NOT_IN_LINK}'

AUTHOR_NOISE_RE = (
    r'(?is)(?:'
    rf'{COMMENT_RE}'
    rf'|{REF_RE}|&lt;ref.*'
    r'|&lt;/?small\s*&gt;|&(?:amp;)?(?:quot|nbsp);'
    r'|\{\{[^|{}\n]*\|?|\}\}'
    r'|\|[^\[\]]*\]\]|[\[\]]'
    r'|\'{2,}|"{2,}|\*|\([^)]*\)'
    r'|[^\S\n]'
    r')+'
)
AUTHOR_SEP_RE = r'(?i)\s*(?:&lt;br\s*/?&gt;|&amp;|\band\b|[,|\n])\s*'

TITLE_NOISE_RE = r'&(?:\w+|#\d+);|[^\p{L}\p{N}]'

AUTO_SUMMARY_RE = r'\s*\(This is an automatically generated summary\.\)'

TEXT_TEMPLATE_NAMES = (
    r'(?:nihongo\d?|nowrap|nobr|not a typo|ill|small|smaller|smallcaps|sc|em|strong|vanchor|center|centre)'
)
QUOTE_TEMPLATE_NAMES = (
    r'(?:blockquote|block quote|quote|cquote|quotation|rquote|quote box|quote frame|poem quote|pull quote'
    r'|centered pull quote)'
)
TEMPLATE_RE = (
    r'(?i)\{\{\s*(?:'
    + TEXT_TEMPLATE_NAMES + r'\s*\|'
    + r'|lang\s*\|[^|{}]*\|'
    + QUOTE_TEMPLATE_NAMES + r'\s*(?:\|\s*(?!(?:text|quote|1)\s*=)[\w-]+\s*=[^|{}]*)*\|\s*(?:(?:text|quote|1)\s*=)?'
    + r')\s*([^|{}]*?)\s*(?:\|[^{}]*)?\}\}'
    + r"|\{\{\s*(')\s*\}\}"
    + r'|\{\{(?!\s*(?:' + TEXT_TEMPLATE_NAMES + '|' + QUOTE_TEMPLATE_NAMES + r'|lang)\s*[|}])[^{}]*\}\}'
)
INNER_TEMPLATE_RE = r'\{\{[^{}]*\}\}'

PLOT_CLEANUP = [
    (
        r'(?is:' + COMMENT_RE + '|' + REF_RE + r'|&lt;gallery\b' + TAG_BODY + r'&gt;.*?&lt;/gallery\s*&gt;)'
        + r'|(?ms:^\{\|.*?^\|\})|' + SPACE_HTML_RE,
        ' ',
    ),
    (LINK_RE, '$1'), (LINK_RE, '$1'),
    (TEMPLATE_RE, '$1$2'), (TEMPLATE_RE, '$1$2'), (TEMPLATE_RE, '$1$2'),
    (INNER_TEMPLATE_RE, ''), (INNER_TEMPLATE_RE, ''),
    (r'\[(?:https?:)?//[^\s\]]*\s*([^\]]*)\]', '$1'),
    (
        r"(?m:^[ \t]*=+[^=\n]*=+[ \t]*$|^[ \t]*[*#:;]+[ \t]*)|https?://[^\s\]|}]+|'{2,}|__[A-Z]+__|" + ENTITY_RE,
        '$1',
    ),
    (r'(?:\[\[|\]\]|\{\{|\}\}|\||\s)+', ' '),
    (r'\(\s*[,;:]?\s*\)', ''),
    (r'(\()\s+|\s+([.,;:!?)])', '$1$2'),
]


def title_key(col):
    return F.regexp_replace(col, TITLE_NOISE_RE, '')


def infobox_field(name: str):
    return F.regexp_extract(
        'value',
        rf'(?ms)^\s*\|[ \t]*{name}[ \t]*=[ \t]*(.*?)\s*(?=^\s*\||^\s*\}}\}}|\n\s*\n|\s\|\s*\w+\s*=|\z)',
        1,
    )


def remove_html(col):
    col = F.regexp_replace(col, SPACE_HTML_RE, ' ')
    return F.regexp_replace(col, ENTITY_RE, '$1')


def squash_spaces(col):
    return F.trim(F.regexp_replace(col, r'\s+', ' '))


def clean_plot(col):
    for pattern, replacement in PLOT_CLEANUP:
        col = F.regexp_replace(col, pattern, replacement)
    return F.trim(col)


def value_list(col):
    col = F.regexp_replace(col, VALUE_NOISE_RE, '')
    col = F.regexp_replace(col, BR_RE, '\n')
    col = remove_html(col)
    col = F.regexp_replace(col, LIST_SEP_RE, '; ')
    col = F.regexp_replace(col, LINK_RE, '$1')
    col = F.regexp_replace(col, r"'{2,}|[ \t]+(?= )", '')
    col = F.regexp_replace(col, r'(?:\s*;\s*)+', '; ')
    col = F.regexp_replace(col, r'^[;\s*]+|[;\s*]+$', '')
    return F.array_remove(F.split(col, '; '), '')


def clean_author(col):
    col = F.regexp_replace(col, AUTHOR_NOISE_RE, ' ')
    authors = F.transform(F.split(col, AUTHOR_SEP_RE), lambda a: squash_spaces(remove_html(a)))
    return F.array_remove(authors, '')


DAY_PATTERNS = ['MMMM d yyyy', 'd MMMM yyyy', 'MMM d yyyy', 'd MMM yyyy']
MONTH_PATTERNS = ['MMMM yyyy', 'MMM yyyy', 'yyyy MMMM']
YEAR_PATTERNS = ['yyyy']


def parse_date(col, patterns):
    return F.coalesce(*[F.to_date(F.try_to_timestamp(col, F.lit(p))) for p in patterns])


def published_range(col):
    col = F.regexp_replace(col, r'^[A-Za-z]+day\b|(?<=\d)(?:st|nd|rd|th)\b|(?<=Sep)t\b', '')
    col = F.trim(F.regexp_replace(col, r'[\s,]+', ' '))

    day = parse_date(col, DAY_PATTERNS)
    month = parse_date(col, MONTH_PATTERNS)
    year = parse_date(col, YEAR_PATTERNS)
    return [
        F.coalesce(day, month, year).alias('published_from'),
        F.coalesce(day, F.last_day(month), F.make_date(F.year(year), F.lit(12), F.lit(31))).alias('published_to'),
    ]


def first_value(wiki, crawled):
    return F.coalesce(F.nullif(F.col(wiki), F.lit('')), F.nullif(F.col(crawled), F.lit('')))


def as_list(col):
    return F.coalesce(col, F.array().cast('array<string>'))


def distinct_by_key(arr):
    def key(c):
        return F.lower(title_key(c))

    return F.filter(arr, lambda x, i: ~F.exists(F.slice(arr, F.lit(1), i), lambda y: key(y) == key(x)))


def flip_name(col):
    col = F.regexp_replace(col, r',[^,]*\d[^,]*$', '')
    return F.trim(F.regexp_replace(col, r'^([^,]+),\s*([^,]+)$', '$2 $1'))


def merge_sources(df):
    crawled_author = F.array_compact(F.array(F.nullif(flip_name(F.col('crawled_author')), F.lit(''))))
    crawled_subjects = F.array_remove(F.split(F.nullif(F.col('crawled_subjects'), F.lit('')), '; '), '')

    authors = F.concat(as_list(F.col('wiki_author')), as_list(crawled_author))
    subjects = F.concat(as_list(F.col('genre')), as_list(crawled_subjects))

    return df.select(
        'title_key',
        first_value('wiki_title', 'crawled_title').alias('title'),
        distinct_by_key(authors).alias('authors'),
        first_value('wiki_language', 'crawled_language').alias('language'),
        distinct_by_key(subjects).alias('subjects'),
        as_list(F.col('book_categories')).alias('categories'),
        F.col('crawled_reading_level').alias('reading_level'),
        'published',
        F.coalesce('published_from', 'crawled_release_from').alias('published_from'),
        F.coalesce('published_to', 'crawled_release_to').alias('published_to'),
        F.col('crawled_abstract').alias('abstract'),
        'plot',
        F.coalesce('from_wiki', F.lit(False)).alias('from_wiki'),
        F.coalesce('from_crawl', F.lit(False)).alias('from_crawl'),
    )


def transform_for_lucene(df):
    df = df.select(
        F.monotonically_increasing_id().alias('id'),
        *[
            F.nullif(F.col(f.name), F.lit('')).alias(f.name) if isinstance(f.dataType, StringType) else f.name
            for f in df.schema.fields
        ],
    )
    return df.withColumn('reading_level', F.col('reading_level').try_cast('double'))


def save_single_json(df, name: str) -> None:
    tmp_dir = f'{name}_tmp'
    df.coalesce(1).write.mode('overwrite').option('ignoreNullFields', False).json(f'{CONTAINER_OUTPUT_DIR}/{tmp_dir}')

    host_tmp_dir = os.path.join(HOST_OUTPUT_DIR, tmp_dir)
    part_file = glob.glob(os.path.join(host_tmp_dir, 'part-*.json'))[0]
    os.replace(part_file, os.path.join(HOST_OUTPUT_DIR, name))
    shutil.rmtree(host_tmp_dir)


def save_single_csv_debug(df, name: str) -> None:
    arrays = [f.name for f in df.schema.fields if isinstance(f.dataType, ArrayType)]
    df = df.withColumns({c: F.to_json(c) for c in arrays})

    tmp_dir = f'{name}_tmp'
    (
        df.coalesce(1)
        .write.mode('overwrite')
        .option('header', True)
        .option('sep', '\t')
        .option('quote', '"')
        .option('escape', '"')
        .csv(f'{CONTAINER_OUTPUT_DIR}/{tmp_dir}')
    )

    host_tmp_dir = os.path.join(HOST_OUTPUT_DIR, tmp_dir)
    part_file = glob.glob(os.path.join(host_tmp_dir, 'part-*.csv'))[0]
    os.replace(part_file, os.path.join(HOST_OUTPUT_DIR, name))
    shutil.rmtree(host_tmp_dir)


def main():
    spark = SparkSession.builder.remote(SPARK_REMOTE).getOrCreate()
    spark.addArtifacts(os.path.abspath(__file__), pyfile=True)

    spark.conf.set('spark.sql.files.maxPartitionBytes', MAX_PARTITION_BYTES)
    spark.conf.set('spark.sql.files.minPartitionNum', MIN_PARTITIONS)

    spark.conf.set('spark.sql.legacy.timeParserPolicy', 'CORRECTED')

    pages = spark.read.option('lineSep', '</page>').text(WIKI_DUMPS)

    published = F.regexp_replace(
        F.regexp_extract(
            F.regexp_replace(
                infobox_field('(?:published|publication_date|pub_date|release_date)'), r'&(?:amp;)*nbsp;', ' '
            ),
            r'^[\p{L}\p{N} ,]*', 0,
        ),
        r'[ ,]+$', '',
    )

    books = (
        pages
        .where(F.col('value').contains('<ns>0</ns>'))
        .where(~F.col('value').contains('<redirect'))
        .where(F.col('value').rlike(BOOK_INFOBOX_RE) | F.col('value').rlike(BOOK_CATEGORY_RE))
        .select(
            squash_spaces(remove_html(F.regexp_extract('value', r'<title>(.*?)</title>', 1))).alias('wiki_title'),
            F.transform(
                F.regexp_extract_all('value', F.lit(BOOK_CATEGORY_RE), 1), lambda c: squash_spaces(remove_html(c))
            ).alias('book_categories'),
            clean_author(infobox_field('author')).alias('wiki_author'),
            F.array_join(value_list(infobox_field('language')), ', ').alias('wiki_language'),
            value_list(infobox_field('genre')).alias('genre'),
            published.alias('published'),
            *published_range(published),
            clean_plot(F.regexp_extract('value', PLOT_RE, 2)).alias('plot'),
        )
        .withColumn('title_key', title_key('wiki_title'))
        .withColumn('from_wiki', F.lit(True))
        .dropDuplicates(['wiki_title'])
    )

    crawled = (
        spark.read
        .option('header', True)
        .option('sep', '\t')
        .option('quote', '"')
        .option('escape', '"')
        .option('multiLine', True)
        .csv(BOOKS_CSV)
        .select(
            F.col("title").alias("crawled_title"),
            title_key("title").alias("title_key"),
            F.col("author").alias("crawled_author"),
            F.col("reading_level").alias("crawled_reading_level"),
            F.col("language").alias("crawled_language"),
            F.col("subjects").alias("crawled_subjects"),
            F.col("machine_release_date").try_cast('date').alias("crawled_release_from"),
            F.col("machine_release_date").try_cast('date').alias("crawled_release_to"),
            F.trim(F.regexp_replace("abstract", AUTO_SUMMARY_RE, '')).alias("crawled_abstract"),
            F.lit(True).alias("from_crawl"),
        )
    )

    joined = crawled.join(books, on='title_key', how='outer')

    documents = transform_for_lucene(merge_sources(joined))
    save_single_json(documents, JOIN_JSON)

    spark.stop()


if __name__ == '__main__':
    main()
