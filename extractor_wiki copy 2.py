import glob
import os
import shutil

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType


SPARK_REMOTE = "sc://localhost:15002"   # Spark Connect port exposed by docker-compose

# paths as seen by the Spark container (see docker-compose volumes)
CONTAINER_INPUT_DIR = "/data"        # <- ../crawler_data
CONTAINER_OUTPUT_DIR = "/output"     # <- ./data

# WIKI_DUMP = f'{CONTAINER_OUTPUT_DIR}/wiki/enwiki-latest-pages-articles.xml.bz2'
BOOKS_CSV = f'{CONTAINER_OUTPUT_DIR}/books_extracted/books_wiki.csv'
# snippets of the dump, read together as one dataframe (same as a union of them)
WIKI_DUMPS = [
    f'{CONTAINER_OUTPUT_DIR}/wiki/wiki-sample.bz2',
    f'{CONTAINER_OUTPUT_DIR}/wiki/wiki-sample3.bz2',       # wiki-sample2 is the same file as articles11 p6899367p7054859
    f'{CONTAINER_OUTPUT_DIR}/wiki/enwiki-latest-pages-articles11.xml-p5399367p6899366.bz2',
    f'{CONTAINER_OUTPUT_DIR}/wiki/enwiki-latest-pages-articles11.xml-p6899367p7054859.bz2',
    f'{CONTAINER_OUTPUT_DIR}/wiki/enwiki-latest-pages-articles14.xml-p13159683p14324602.bz2',
    f'{CONTAINER_OUTPUT_DIR}/wiki/enwiki-latest-pages-articles15.xml-p14324603p15824602.bz2',
    # f'{CONTAINER_OUTPUT_DIR}/wiki/enwiki-latest-pages-articles.xml.bz2'
]

# the same ./data folder as seen by this script, on the host
HOST_OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
JOIN_JSON = 'wiki_join_craw_attempt4.jsonl'

# bzip2 is a splittable codec: Spark cuts the compressed file into byte ranges, finds the next
# bzip2 block boundary in each range and decompresses the ranges in parallel (one task each).
# default max chunk is 128MB (300MB dump = ~3 tasks); smaller chunks = more tasks.
MAX_PARTITION_BYTES = 16 * 1024 * 1024   # ~19 tasks for the 300MB sample
MIN_PARTITIONS = 18                       # lower bound on the number of read tasks

# {{Infobox book/novel/short story/poem ...}}, any case/spacing, but not {{Infobox book series}}
# novellas and poetry collections use {{Infobox book}}, they have no infobox of their own
BOOK_INFOBOX_RE = r'(?i)\{\{\s*infobox[ _]+(?:book|novel|short[ _]+story|poem)\s*(\||\n|\}\})'

# [[Category:... books|novels|novellas|poetry collections|short story collections]]
# the type has to end the category name ("1945 British novels", "Portuguese poetry collections"),
# categories of things *about* or *based on* books are skipped ("Films based on British novels")
BOOK_TYPES = r'books|novels|novellas|poetry collections|short story collections'
BOOK_CATEGORY_EXCLUDE = r'based on|adaptations? of|characters|lists? of|series'
BOOK_CATEGORY_RE = (
    r'(?i)\[\[\s*Category\s*:\s*'
    rf'(?![^\]|]*\b(?:{BOOK_CATEGORY_EXCLUDE})\b)'
    rf'([^\]|]*?\b({BOOK_TYPES}))\s*(?:\|[^\]]*)?\]\]'
)

# == Plot ==, == Plot summary ==, ... group 2 is the section text, subsections stay inside it,
# it ends at the next heading of the same level, at the next == heading or at the end of the page
PLOT_RE = (
    r'(?ims)^(={2,})[ \t]*[^=\n]*\bplot\b[^=\n]*?[ \t]*\1[ \t]*$\s*'
    r'(.*?)\s*(?=^\1[^=]|^==[^=]|</text>|\z)'
)

# [[Target]] or [[Target|Label]], group 1 is the text shown on the page (Label if there is one)
WIKI_LINK_RE = r'\[\[(?:[^\]]*\|)?([^\]|]*)\]\]'

# comments, <ref>s and the list templates around the values, the dump has < and > escaped
VALUE_NOISE_RE = (
    r'(?is)&lt;!--.*?--&gt;|&lt;ref[^&]*?/&gt;|&lt;ref.*?&lt;/ref&gt;'
    r'|\{\{\s*(?:plainlist|flatlist|hlist|ubl|unbulleted[ _]list)\s*|\}\}'
)

# not inside a link, so [[Sword and sorcery]] and [[A|B]] stay in one piece
NOT_IN_LINK = r'(?![^\[\]]*\]\])'
# , ; | new line <br> and * bullets, but not the comma of 'Name, Jr.'
LIST_SEP_RE = rf'\s*(?:,(?!\s*[JS]r\b)|[;|\n]|&lt;br\s*/?&gt;)[\s*]*{NOT_IN_LINK}'

# everything but letters and digits, and xml entities as a whole, wiki titles have '&amp;' or '&quot;'
TITLE_NOISE_RE = r'&(?:\w+|#\d+);|[^\p{L}\p{N}]'


def title_key(col):
    """Title with only letters and digits left, the two sources are joined on it ('A, B: c.' -> 'ABc')."""
    return F.regexp_replace(col, TITLE_NOISE_RE, '')


def infobox_field(name: str):
    """Value of '| name = ...' inside the infobox, can span lines (empty if the field is empty).

    It ends at the next '| field' line, at the }} line, at an empty line or at '| field =' on the same line.
    """
    return F.regexp_extract(
        'value',
        rf'(?ms)^\s*\|[ \t]*{name}[ \t]*=[ \t]*(.*?)\s*(?=^\s*\||^\s*\}}\}}|\n\s*\n|\s\|\s*\w+\s*=|\z)',
        1,
    )


def value_list(col):
    """Values of an infobox field without wiki markup, as a list ('[[A|B]], c' -> ['B', 'c'])."""
    col = F.regexp_replace(col, VALUE_NOISE_RE, '')
    col = F.regexp_replace(col, LIST_SEP_RE, '; ')
    col = F.regexp_replace(col, WIKI_LINK_RE, '$1')
    col = F.regexp_replace(col, r'(?:\s*;\s*)+', '; ')      # empty values between separators
    col = F.regexp_replace(col, r'^[;\s*]+|[;\s*]+$', '')
    return F.array_remove(F.split(col, '; '), '')               # '' -> [''] would leave an empty one


def clean_author(col):
    """'[[A (writer)]] and [[B]]' -> ['A', 'B'] (empty list if there is no author)"""
    # <!-- comments -->, also the cut off '\n<!--' at the end of a value (Pale Fire), they are notes for editors
    col = F.regexp_replace(col, r'(?s)&lt;!--.*?(?:--&gt;|$)', '')
    # <ref>s: self-closing <ref name=x />, a whole <ref>...</ref>, or a cut off <ref ... to the end of the value,
    # the citation is not an author ('Dick King-Smith<ref>{{cite news|...}}</ref>' -> 'Dick King-Smith')
    col = F.regexp_replace(col, r'(?s)&lt;ref[^&]*?/&gt;|&lt;ref.*?&lt;/ref&gt;|&lt;ref.*', '')
    # &quot; and &nbsp; (also double escaped &amp;nbsp;) removed, before &amp; becomes ',' below,
    # otherwise 'C.&amp;nbsp;S. Lewis' would be split into 'C.' and 'nbsp;S. Lewis'
    col = F.regexp_replace(col, r'&(?:amp;)?(?:quot|nbsp);', '')
    # <br>, <br/>, <br /> put each author on its own line -> ','
    col = F.regexp_replace(col, r'(?i)\s*&lt;br\s*/?&gt;\s*', ',')
    # 'A &amp; B' are two authors -> ','
    col = F.regexp_replace(col, r'\s*&amp;\s*', ',')
    # <small> and </small> only make the text smaller, what is inside belongs to the same author (mostly a note
    # like '(ed.)', removed with the other brackets below) -> ' ', a space and not '' so no two words get glued,
    # and not ',' so the note does not become an author of its own
    col = F.regexp_replace(col, r'(?i)&lt;/?small\s*&gt;', ' ')
    # '' italics and ''' bold of the wiki markup (and "" ), the name stays
    col = F.regexp_replace(col, r'\'{2,}|"{2,}', '')
    # if the value is a {{ubl|A|B}} / {{unbulleted list|A|B}} list, the authors are separated by '|':
    # drop the template around them and turn only the '|' outside [[links]] into ',', so the '|' of
    # [[Target|Label]] stays for later, has to be done before [ and ] are removed
    is_ubl = col.rlike(r'(?i)^\s*\{\{\s*(?:ubl|unbulleted[ _]list)\s*\|')
    ubl = F.regexp_replace(col, r'(?i)^\s*\{\{\s*(?:ubl|unbulleted[ _]list)\s*\||\}\}\s*$', '')
    ubl = F.regexp_replace(ubl, r'\|(?![^\[\]]*\]\])', ',')
    col = F.when(is_ubl, ubl).otherwise(col)
    # if the value is a {{plainlist|\n* A\n* B}} / {{Plain list ...}} list (or just '* A\n* B' bullet lines without
    # the template), the authors are one per line after a '*': drop the template around them and turn every
    # new line with its '*' into ','; the closing }} can be missing when infobox_field cut the value at a '}}' line
    is_plainlist = col.rlike(r'(?i)^\s*(?:\{\{\s*plain\s*list|\*)')
    plainlist = F.regexp_replace(col, r'(?i)^\s*\{\{\s*plain\s*list\s*\|?|\}\}\s*$', '')
    plainlist = F.regexp_replace(plainlist, r'\s*\n\s*\*?\s*|^\s*\*\s*', ',')
    col = F.when(is_plainlist, plainlist).otherwise(col)
    # leftover new lines, tabs, \r and runs of spaces -> one space, after the plainlist step that needs the new lines
    col = F.regexp_replace(col, r'\s+', ' ')
    # '|Label]]' of [[Target|Label]] (the target is kept, its '(writer)' is cut below), then the other [ and ]
    col = F.regexp_replace(col, r'\|[^\[\]]*\]\]|[\[\]]', '')
    col = F.regexp_replace(col, r'\s+and\s+', ', ')            # ' and ' -> ', ', not the 'and' in 'Alexander'
    col = F.regexp_replace(col, r'\s*\([^)]*\)', '')           # '(...)' with the brackets
    # one author per comma, spaces around cut off; the lambda is needed, trim(col, trim) has 2 parameters
    # and transform would pass the element index as the second one
    authors = F.transform(F.split(col, ','), lambda a: F.trim(a))
    return F.array_remove(authors, '')                         # '' -> [''] and 'A,,B' would leave empty ones


def save_single_json(df, name: str) -> None:
    """Spark writes a folder with a part-* file, so move that file to ./data/<name> and drop the folder.

    JSON lines (one book per line), unlike csv it keeps array columns as lists.
    """
    tmp_dir = f'{name}_tmp'
    df.coalesce(1).write.mode('overwrite').json(f'{CONTAINER_OUTPUT_DIR}/{tmp_dir}')

    host_tmp_dir = os.path.join(HOST_OUTPUT_DIR, tmp_dir)
    part_file = glob.glob(os.path.join(host_tmp_dir, 'part-*.json'))[0]
    os.replace(part_file, os.path.join(HOST_OUTPUT_DIR, name))
    shutil.rmtree(host_tmp_dir)


def save_single_csv_debug(df, name: str) -> None:
    """Same as save_single_json but a tab separated csv, only to look at the data while debugging.

    csv can't store array columns, so each one is written as its json text: ["A","B"], [] when empty,
    so it is still visible where one value ends and the next starts.
    """
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

    pages = spark.read.option('lineSep', '</page>').text(WIKI_DUMPS)


    books = (
        pages
        # articles only: plain substring checks are much cheaper than a regex, a page has a single <ns>
        # in its header and < > inside the article text are escaped as &lt; &gt;, so this cannot match the text
        .where(F.col('value').contains('<ns>0</ns>'))
        .where(~F.col('value').contains('<redirect'))
        # book pages: rlike stops at the first match and builds nothing, the categories are extracted
        # below only for the pages that are left; with | the category regex runs only when there is no infobox
        .where(F.col('value').rlike(BOOK_INFOBOX_RE) | F.col('value').rlike(BOOK_CATEGORY_RE))
        .select(
            F.regexp_extract('value', r'<title>(.*?)</title>', 1).alias('wiki_title'),
            F.regexp_extract_all('value', F.lit(BOOK_CATEGORY_RE), 1).alias('book_categories'),
            clean_author(infobox_field('author')).alias('wiki_author'),     # author, language are also in the csv
            # [[English language|English]] -> English, [[English]] -> English
            F.regexp_replace(infobox_field('language'), WIKI_LINK_RE, '$1').alias('wiki_language'),
            value_list(infobox_field('genre')).alias('genre'),
            # keep letters, digits, spaces and commas from the start, cut at the first other character
            # ('17 August 1945 ([[Secker..' -> '17 August 1945'), then drop a ', ' left at the end
            F.regexp_replace(
                # older articles use the other names of the field ('| publication_date = 1964')
                F.regexp_extract(
                    infobox_field('(?:published|publication_date|pub_date|release_date)'), r'^[\p{L}\p{N} ,]*', 0
                ),
                r'[ ,]+$', '',
            ).alias('published'),
            F.regexp_extract('value', PLOT_RE, 2).alias('plot'),
        )
        .withColumn('title_key', title_key('wiki_title'))
        .dropDuplicates(['wiki_title'])     # the snippets can overlap, keep each page once
    )

    # same options as the write in extractor_crawled_books.py
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
            F.col("release_date").alias("crawled_release_date"),
            F.col("abstract").alias("crawled_abstract"),
        )
    )


    # crawled.show(30, truncate=50)
    # books.show(30, truncate=50)

    joined = crawled.join(books, on='title_key', how='outer')

    # joined.show(30, truncate=50)

    # save_single_json(joined, JOIN_JSON)
    # debugging only: a 2nd write runs the whole job (reading the dumps) again, cache joined first if both are on
    save_single_csv_debug(joined, JOIN_JSON.replace('.jsonl', '_debug.csv'))

    spark.stop()


if __name__ == '__main__':
    main()
