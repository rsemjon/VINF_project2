import glob
import os
import shutil

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


SPARK_REMOTE = "sc://localhost:15002"   # Spark Connect port exposed by docker-compose

# paths as seen by the Spark container (see docker-compose volumes)
CONTAINER_INPUT_DIR = "/data"        # <- ../crawler_data
CONTAINER_OUTPUT_DIR = "/output"     # <- ./data

WIKI_DUMP = f'{CONTAINER_OUTPUT_DIR}/wiki/wiki-sample2.bz2'
BOOKS_CSV = f'{CONTAINER_OUTPUT_DIR}/books_extracted/books_wiki.csv'

# the same ./data folder as seen by this script, on the host
HOST_OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
JOIN_CSV = 'wiki_join_craw_attempt2.csv'

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
# also 'A and B', 'A, B and C', 'A & B' - only for names, a plain 'Sword and sorcery' genre is one value
NAME_SEP_RE = rf',?\s+(?:and|&amp;)\s+{NOT_IN_LINK}|{LIST_SEP_RE}'


def infobox_field(name: str):
    """Value of '| name = ...' inside the infobox, can span lines (empty if the field is empty).

    It ends at the next '| field' line, at the }} line, at an empty line or at '| field =' on the same line.
    """
    return F.regexp_extract(
        'value',
        rf'(?ms)^\s*\|[ \t]*{name}[ \t]*=[ \t]*(.*?)\s*(?=^\s*\||^\s*\}}\}}|\n\s*\n|\s\|\s*\w+\s*=|\z)',
        1,
    )


def value_list(col, sep_re: str = LIST_SEP_RE):
    """Values of an infobox field without wiki markup, joined by '; ' ('[[A|B]], c' -> 'B; c')."""
    col = F.regexp_replace(col, VALUE_NOISE_RE, '')
    col = F.regexp_replace(col, sep_re, '; ')
    col = F.regexp_replace(col, WIKI_LINK_RE, '$1')
    col = F.regexp_replace(col, r'(?:\s*;\s*)+', '; ')      # empty values between separators
    return F.regexp_replace(col, r'^[;\s*]+|[;\s*]+$', '')


def save_single_csv(df, name: str) -> None:
    """Spark writes a folder with a part-* file, so move that file to ./data/<name> and drop the folder."""
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

    pages = spark.read.option('lineSep', '</page>').text(WIKI_DUMP)

    books = (
        pages
        .where(F.regexp_extract('value', r'<ns>(\d+)</ns>', 1) == '0')
        .where(~F.col('value').contains('<redirect'))
        .select(
            '*',
            F.col('value').rlike(BOOK_INFOBOX_RE).alias('has_infobox'),
            F.regexp_extract_all('value', F.lit(BOOK_CATEGORY_RE), 1).alias('book_categories'),
            F.lower(F.regexp_extract('value', BOOK_CATEGORY_RE, 2)).alias('book_type'),
        )
        .where(F.col('has_infobox') | (F.size('book_categories') > 0))
        .select(
            F.regexp_extract('value', r'<title>(.*?)</title>', 1).alias('title'),
            'book_categories',
            value_list(infobox_field('author'), NAME_SEP_RE).alias('wiki_author'),     # author, language are also in the csv
            value_list(infobox_field('language'), NAME_SEP_RE).alias('wiki_language'),
            value_list(infobox_field('genre')).alias('genre'),
            infobox_field('published').alias('published'),
            F.regexp_extract('value', PLOT_RE, 2).alias('plot'),
        )
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
            "title",
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

    joined = crawled.join(books, on='title', how='outer')

    joined.show(30, truncate=50)

    # csv can't store array columns
    save_single_csv(joined.withColumn('book_categories', F.concat_ws('; ', 'book_categories')), JOIN_CSV)

    spark.stop()


if __name__ == '__main__':
    main()
