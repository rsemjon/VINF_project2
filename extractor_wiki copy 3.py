import glob
import os
import shutil

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType


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

# html inside the article text, the dump has < > " & escaped: <ref name="a" /> is &lt;ref name=&quot;a&quot; /&gt;
# the inside of a tag up to its &gt;, it can have & in it (the &quot; of name=&quot;a&quot;), so not [^&]*
TAG_BODY = r'[^&]*(?:&(?!gt;)[^&]*)*'
# <!-- comment -->, also a cut off one that runs to the end
COMMENT_RE = r'&lt;!--.*?(?:--&gt;|\z)'
# <ref name="a" /> and <ref ...>...</ref>, (?<!/) so the 2nd one does not take a self-closing ref
REF_RE = rf'&lt;ref\b{TAG_BODY}/&gt;|&lt;ref\b{TAG_BODY}(?<!/)&gt;.*?&lt;/ref\s*&gt;'
# any other tag, opening or closing (<br />, <blockquote>, </poem>, <sup>), the text between the tags stays
ESCAPED_TAG_RE = rf'&lt;/?[a-zA-Z]{TAG_BODY}&gt;'
BR_RE = r'(?i)&lt;br\s*/?\s*&gt;'
# &nbsp; &ndash; ... stand between two words -> one space; the wiki source also has them double escaped
# (&amp;nbsp;), a plain &amp; is the '&' of the text ('Harper &amp; Row' -> 'Harper & Row'),
# every other entity (&quot; &#124;) is removed
SPACE_ENTITY_RE = r'&(?:amp;)*(?:nbsp|ensp|emsp|thinsp|ndash|mdash|minus);'
# &amp; that is not the escaped & of another entity (&amp;nbsp; &amp;quot;)
AMP_RE = r'&amp;(?!(?:amp;)*(?:[a-zA-Z]+\d*|#\d+|#[xX][0-9a-fA-F]+);)'
ENTITY_RE = r'&(?:amp;)*(?:[a-zA-Z]+\d*|#\d+|#[xX][0-9a-fA-F]+);'

# comments, <ref>s and the list templates around the values
VALUE_NOISE_RE = (
    rf'(?is){COMMENT_RE}|{REF_RE}'
    r'|\{\{\s*(?:plainlist|flatlist|hlist|ubl|unbulleted[ _]list)\s*|\}\}'
)

# not inside a link, so [[Sword and sorcery]] and [[A|B]] stay in one piece
NOT_IN_LINK = r'(?![^\[\]]*\]\])'
# , ; | new line and * bullets, but not the comma of 'Name, Jr.'; <br> is turned into a new line before
# (value_list), the entities are gone by then, so the ; of &quot; is never taken for a separator
LIST_SEP_RE = rf'\s*(?:,(?!\s*[JS]r\b)|[;|\n])[\s*]*{NOT_IN_LINK}'

# author field, see notes.md: every piece of the value is either noise (-> one space) or a separator (-> split)
# markup that is never part of a name, a run of it together with spaces and tabs -> one space
AUTHOR_NOISE_RE = (
    r'(?is)(?:'
    rf'{COMMENT_RE}'                                            # <!-- comments -->, also cut off ones
    rf'|{REF_RE}|&lt;ref.*'                                     # <ref>s: self-closing, whole, cut off
    r'|&lt;/?small\s*&gt;|&(?:amp;)?(?:quot|nbsp);'            # <small>, &quot; &nbsp; (not a bare &amp;)
    r'|\{\{[^|{}\n]*\|?|\}\}'                                  # any template name ({{ubl|, {{plainlist) and }}
    r'|\|[^\[\]]*\]\]|[\[\]]'                                  # |Label]] of [[Target|Label]], then [ ]
    r'|\'{2,}|"{2,}|\*|\([^)]*\)'                              # '' ''' italics/bold, * bullets, (writer)
    r'|[^\S\n]'                                                # spaces and tabs, but not new lines
    r')+'
)
# what stands between two authors, with the spaces and new lines around it
AUTHOR_SEP_RE = r'(?i)\s*(?:&lt;br\s*/?&gt;|&amp;|\band\b|[,|\n])\s*'

# everything but letters and digits, and xml entities as a whole, wiki titles have '&amp;' or '&quot;'
TITLE_NOISE_RE = r'&(?:\w+|#\d+);|[^\p{L}\p{N}]'

# the crawled abstracts end with it, it is in almost every one, so it would only add noise to the search
AUTO_SUMMARY_RE = r'\s*\(This is an automatically generated summary\.\)'

# templates whose first parameter is text of the article: {{Nihongo|Renmaru|雅堂 錬丸|Miyabidō Renmaru}} -> Renmaru
TEXT_TEMPLATE_NAMES = (
    r'(?:nihongo\d?|nowrap|nobr|not a typo|ill|small|smaller|smallcaps|sc|em|strong|vanchor|center|centre)'
)
TEXT_TEMPLATE_RE = rf'(?i)\{{\{{\s*{TEXT_TEMPLATE_NAMES}' r'\s*\|\s*([^|{}]*?)\s*(?:\|[^{}]*)?\}\}'
# {{lang|fr|texte}} -> texte
LANG_TEMPLATE_RE = r'(?i)\{\{\s*lang\s*\|[^|{}]*\|\s*([^|{}]*?)\s*(?:\|[^{}]*)?\}\}'

# quotes, a plot is sometimes a quoted book jacket: {{blockquote|text|author}}, {{quote box|width=30em|quote=text}}
QUOTE_TEMPLATE_NAMES = (
    r'(?:blockquote|block quote|quote|cquote|quotation|rquote|quote box|quote frame|poem quote|pull quote'
    r'|centered pull quote)'
)
# only the text stays: the 1st parameter without a name or the one named text= / quote= / 1=; the named ones
# before it (width=, align=) and everything after it (the author, the source) go
QUOTE_TEMPLATE_RE = (
    rf'(?i)\{{\{{\s*{QUOTE_TEMPLATE_NAMES}\s*'
    r'(?:\|\s*(?!(?:text|quote|1)\s*=)[\w-]+\s*=[^|{}]*)*'           # named parameters before the text
    r'\|\s*(?:(?:text|quote|1)\s*=\s*)?([^|{}]*)'                     # the text
    r'(?:\|[^{}]*)?\}\}'                                              # the parameters after it
)
# a template with no template inside it, except the ones whose text is kept (above): a quote with a
# {{center|..}} inside is not flat yet, it must not go before its text is taken out (see TEMPLATE_PASS)
OTHER_TEMPLATE_RE = (
    rf'(?i)\{{\{{(?!\s*(?:{TEXT_TEMPLATE_NAMES}|{QUOTE_TEMPLATE_NAMES}|lang)\s*[|}}])' r'[^{}]*\}\}'
)
# any template with no template inside it, for what is left after the passes
INNER_TEMPLATE_RE = r'\{\{[^{}]*\}\}'
# one round over the templates, innermost first: the ones with text give their text, the rest go;
# 3 rounds -> templates nested 3 deep ({{quote box|quote={{center|{{smallcaps|text}}}}}}) are handled
TEMPLATE_PASS = [
    (QUOTE_TEMPLATE_RE, ' $1 '),
    (TEXT_TEMPLATE_RE, '$1'),
    (LANG_TEMPLATE_RE, '$1'),
    (r"\{\{\s*'\s*\}\}", "'"),                                     # {{'}} is an apostrophe
    (OTHER_TEMPLATE_RE, ''),
]
# a link with no link inside it, [[A|B]] -> B (the last part); files and categories are not text, they go below
INNER_LINK_RE = r'(?i)\[\[(?!\s*(?:File|Image|Category)\s*:)(?:[^\[\]|]*\|)*([^\[\]|]*)\]\]'
# [[File:x.jpg|thumb|caption]], the links in the caption are already flat by then
FILE_LINK_RE = r'(?i)\[\[\s*(?:File|Image|Category)\s*:[^\[\]]*\]\]'

# plot section wiki text -> plain text, one (pattern, replacement) after the other, the order matters:
# blocks that are not plot text go whole first, then the tags around text, the links (templates can have
# links inside), the templates, the formatting, the entities (the tags need their &lt; &gt; until they are gone)
PLOT_CLEANUP = [
    # comments, <ref>s (citations, most of the urls and {{cite}}s are inside) and <gallery>s go with their text
    (rf'(?is){COMMENT_RE}|{REF_RE}|&lt;gallery\b{TAG_BODY}&gt;.*?&lt;/gallery\s*&gt;', ' '),
    (r'(?ms)^\{\|.*?^\|\}', ' '),                                  # {| tables |}
    (ESCAPED_TAG_RE, ' '),                                         # <blockquote>, <poem>, <br />: the text stays
    (INNER_LINK_RE, '$1'), (INNER_LINK_RE, '$1'),                  # 2x: [[A|[[B]]]] (rare), the caption links
    (FILE_LINK_RE, ' '),
    *TEMPLATE_PASS, *TEMPLATE_PASS, *TEMPLATE_PASS,
    (INNER_TEMPLATE_RE, ''), (INNER_TEMPLATE_RE, ''),              # nested deeper or not matched above
    (r'\[(?:https?:)?//[^\s\]]*\s*([^\]]*)\]', '$1'),              # [http://x label] -> label
    (r'https?://[^\s\]|}]+', ''),                                  # bare urls
    (r"'{2,}", ''),                                                # ''italic'', '''bold'''
    (r'(?m)^[ \t]*=+[^=\n]*=+[ \t]*$', ''),                        # ===Characters=== headings
    (r'(?m)^[ \t]*[*#:;]+[ \t]*', ''),                             # * bullets, # numbers, : indents
    (r'__[A-Z]+__', ''),                                           # __NOTOC__
    (SPACE_ENTITY_RE, ' '),
    (AMP_RE, '&'),                                                 # 'Tom &amp; Jerry' -> 'Tom & Jerry'
    (ENTITY_RE, ''),                                               # &quot; &lt; &#124; ...
    (r'\[\[|\]\]|\{\{|\}\}|\|', ' '),                              # what is left of broken (cut off) markup
    (r'\s+', ' '),                                                 # new lines too, the plot is one line
    (r'\(\s*[,;:]?\s*\)', ''),                                     # '()' left by a removed template
    (r'\s+([.,;:!?)])', '$1'),                                     # 'word{{cn}}.' -> 'word .' -> 'word.'
    (r'\(\s+', '('),
]


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


def remove_html(col):
    """Escaped html tags and entities out: '&lt;sup&gt;1&lt;/sup&gt;' -> ' 1 ', 'A&nbsp;B' -> 'A B', '&quot;A&quot;' -> 'A',
    but 'Harper &amp; Row' -> 'Harper & Row' (the & is part of the text).

    <ref>s and comments have to go before (REF_RE, COMMENT_RE), here only the tags would go, not their text.
    """
    col = F.regexp_replace(col, ESCAPED_TAG_RE, ' ')
    col = F.regexp_replace(col, SPACE_ENTITY_RE, ' ')
    col = F.regexp_replace(col, AMP_RE, '&')
    return F.regexp_replace(col, ENTITY_RE, '')


def squash_spaces(col):
    """Runs of spaces and new lines -> one space, none at the ends ('Hodder  Stoughton ' -> 'Hodder Stoughton')."""
    return F.trim(F.regexp_replace(col, r'\s+', ' '))


def clean_plot(col):
    """Plot section wiki text -> one line of plain text, see PLOT_CLEANUP ('' when only templates were in it)."""
    for pattern, replacement in PLOT_CLEANUP:
        col = F.regexp_replace(col, pattern, replacement)
    return F.trim(col)


def value_list(col):
    """Values of an infobox field without wiki markup, as a list ('[[A|B]], c' -> ['B', 'c'])."""
    col = F.regexp_replace(col, VALUE_NOISE_RE, '')
    col = F.regexp_replace(col, BR_RE, '\n')            # a separator, it has to stay when the tags go
    col = remove_html(col)                              # before the split, the ; of &quot; is no separator
    col = F.regexp_replace(col, LIST_SEP_RE, '; ')
    col = F.regexp_replace(col, WIKI_LINK_RE, '$1')
    col = F.regexp_replace(col, r"'{2,}|[ \t]+(?= )", '')   # ''italic'', and 2+ spaces -> one
    col = F.regexp_replace(col, r'(?:\s*;\s*)+', '; ')      # empty values between separators
    col = F.regexp_replace(col, r'^[;\s*]+|[;\s*]+$', '')
    return F.array_remove(F.split(col, '; '), '')               # '' -> [''] would leave an empty one


def clean_author(col):
    """'[[A (writer)]] and [[B]]' -> ['A', 'B'] (empty list if there is no author)"""
    # 1. noise -> one space, what is left are names and separators ('[[A (writer)|A]] and [[B]]' -> ' A and B ')
    col = F.regexp_replace(col, AUTHOR_NOISE_RE, ' ')
    # 2. split at the separators, they take the spaces and new lines around them; the lambda is needed,
    # trim(col, trim) has 2 parameters and transform would pass the element index as the second one;
    # the entities go only after the split, &amp; is a separator ('A &amp; B'), '&gt;400 people' -> '400 people'
    authors = F.transform(F.split(col, AUTHOR_SEP_RE), lambda a: squash_spaces(remove_html(a)))
    return F.array_remove(authors, '')                         # '' -> [''] and 'A,,B' would leave empty ones


# handlers for the published date, one Spark datetime pattern each, grouped by how precise the date is;
# they run on the value after published_range() normalized it ('Tuesday, October 30, 2001' -> 'October 30 2001')
DAY_PATTERNS = ['MMMM d yyyy', 'd MMMM yyyy', 'MMM d yyyy', 'd MMM yyyy']   # August 1 1995, 31 March 1945, Oct 30 1989
MONTH_PATTERNS = ['MMMM yyyy', 'MMM yyyy', 'yyyy MMMM']                       # February 1984, Jan 2008, 2000 July
YEAR_PATTERNS = ['yyyy']                                                       # 1971


def parse_date(col, patterns):
    """First pattern that parses the whole value wins, null if none does.

    try_ because Spark 4 runs in ANSI mode, where a plain to_date would fail the whole job on a bad value.
    """
    return F.coalesce(*[F.to_date(F.try_to_timestamp(col, F.lit(p))) for p in patterns])


def published_range(col):
    """Human published text -> [published_from, published_to], the dates the publication can fall between.

    '31 March 1945' -> 1945-03-31 .. 1945-03-31, 'February 1984' -> 1984-02-01 .. 1984-02-29,
    '1971' -> 1971-01-01 .. 1971-12-31, anything else ('9th century', '1989, 2019', 'c') -> null .. null.
    A search for 'published between A and B' is then: published_from <= B and published_to >= A.
    """
    # drop what the patterns can't take: a weekday at the start, the 'th' of '8th', the 't' of 'Sept'
    col = F.regexp_replace(col, r'^[A-Za-z]+day\b|(?<=\d)(?:st|nd|rd|th)\b|(?<=Sep)t\b', '')
    # commas and runs of spaces -> one space ('August 1, 1995' -> 'August 1 1995', 'May, 1970' -> 'May 1970')
    col = F.trim(F.regexp_replace(col, r'[\s,]+', ' '))

    day = parse_date(col, DAY_PATTERNS)
    month = parse_date(col, MONTH_PATTERNS)     # parsed as the 1st of the month
    year = parse_date(col, YEAR_PATTERNS)       # parsed as the 1st of January
    return [
        F.coalesce(day, month, year).alias('published_from'),
        F.coalesce(day, F.last_day(month), F.make_date(F.year(year), F.lit(12), F.lit(31))).alias('published_to'),
    ]


def first_value(wiki, crawled):
    """The wiki value, the crawled one when the wiki has none ('' counts as none, regexp_extract gives '')."""
    return F.coalesce(F.nullif(F.col(wiki), F.lit('')), F.nullif(F.col(crawled), F.lit('')))


def as_list(col):
    """null -> [] (a book from only one source has null in the columns of the other one)."""
    return F.coalesce(col, F.array().cast('array<string>'))


def distinct_by_key(arr):
    """Keeps the first of the values that are the same up to case, spaces and punctuation.

    ['J. R. R. Tolkien', 'J.R.R. Tolkien', 'Ann'] -> ['J. R. R. Tolkien', 'Ann'], so the wiki value wins
    when the wiki values come first. A value is kept when no value before it (slice of the first i) has its key.
    """
    def key(c):
        return F.lower(title_key(c))

    return F.filter(arr, lambda x, i: ~F.exists(F.slice(arr, F.lit(1), i), lambda y: key(y) == key(x)))


def flip_name(col):
    """'Conrad, Joseph' -> 'Joseph Conrad', the crawled site writes 'Last, First', the wiki 'First Last'.

    Life years the crawler left ('Homer, 751? BCE-651? BCE') are dropped first (the part after the last comma
    when it has a digit). A name with more commas ('Smith, John, Jr.') is not flipped, it stays as it is.
    """
    col = F.regexp_replace(col, r',[^,]*\d[^,]*$', '')
    return F.trim(F.regexp_replace(col, r'^([^,]+),\s*([^,]+)$', '$2 $1'))


def merge_sources(df):
    """One column per field instead of a wiki_ and a crawled_ one, the indexer and the search see one value.

    - one value (title, language): the wiki one, the crawled one when the wiki has none
    - lists (authors, subjects): both merged, wiki values first, a value that is in both is kept once
      (see distinct_by_key); crawled_subjects and genre are both 'what the book is about', so they go together
    - date range (published_from, published_to): the wiki one, the crawled one when the wiki has none; a wiki
      range always has both ends or none (published_range), so the two ends never come from different sources
    - fields only one source has keep their value, only the prefix goes away
    """
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
    )


def transform_for_lucene(df):
    """Merged books (see merge_sources) -> documents ready to index, so the indexer does not have to fix the values.

    - id: a unique number for each document (title_key is not unique, crawled books can share a title)
    - '' -> null in every text field: a missing wiki field is '' (regexp_extract found nothing), a missing
      crawled one is null, now both are null and the indexer only has to skip nulls
    - reading_level: the csv is read as text, '8.5' -> 8.5, a number for numeric range queries
    """
    df = df.select(
        F.monotonically_increasing_id().alias('id'),
        *[
            F.nullif(F.col(f.name), F.lit('')).alias(f.name) if isinstance(f.dataType, StringType) else f.name
            for f in df.schema.fields
        ],
    )
    # try_ because Spark 4 runs in ANSI mode, where a plain cast would fail the whole job on a bad value
    return df.withColumn('reading_level', F.col('reading_level').try_cast('double'))


def save_single_json(df, name: str) -> None:
    """Spark writes a folder with a part-* file, so move that file to ./data/<name> and drop the folder.

    JSON lines (one book per line), unlike csv it keeps array columns as lists. ignoreNullFields=False
    writes a null field as "field": null (Spark would leave it out), so every line has the same keys.
    """
    tmp_dir = f'{name}_tmp'
    df.coalesce(1).write.mode('overwrite').option('ignoreNullFields', False).json(f'{CONTAINER_OUTPUT_DIR}/{tmp_dir}')

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

    # a value no pattern fits -> null, never an error about the parser of the old Spark versions
    spark.conf.set('spark.sql.legacy.timeParserPolicy', 'CORRECTED')

    pages = spark.read.option('lineSep', '</page>').text(WIKI_DUMPS)

    # keep letters, digits, spaces and commas from the start, cut at the first other character
    # ('17 August 1945 ([[Secker..' -> '17 August 1945'), then drop a ', ' left at the end;
    # older articles use the other names of the field ('| publication_date = 1964')
    # an &nbsp; is a space there ('August&nbsp;1995' would be cut to 'August'), other entities still cut it
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
        # articles only: plain substring checks are much cheaper than a regex, a page has a single <ns>
        # in its header and < > inside the article text are escaped as &lt; &gt;, so this cannot match the text
        .where(F.col('value').contains('<ns>0</ns>'))
        .where(~F.col('value').contains('<redirect'))
        # book pages: rlike stops at the first match and builds nothing, the categories are extracted
        # below only for the pages that are left; with | the category regex runs only when there is no infobox
        .where(F.col('value').rlike(BOOK_INFOBOX_RE) | F.col('value').rlike(BOOK_CATEGORY_RE))
        .select(
            # 'Tom &amp; Jerry' -> 'Tom Jerry', title_key below comes out the same as from the raw title
            squash_spaces(remove_html(F.regexp_extract('value', r'<title>(.*?)</title>', 1))).alias('wiki_title'),
            # 'Hodder &amp; Stoughton books' -> 'Hodder Stoughton books'
            F.transform(
                F.regexp_extract_all('value', F.lit(BOOK_CATEGORY_RE), 1), lambda c: squash_spaces(remove_html(c))
            ).alias('book_categories'),
            clean_author(infobox_field('author')).alias('wiki_author'),     # author, language are also in the csv
            # [[English language|English]] -> English, '[[Welsh]]&lt;br/&gt;English&lt;ref../&gt;' -> 'Welsh, English'
            F.array_join(value_list(infobox_field('language')), ', ').alias('wiki_language'),
            value_list(infobox_field('genre')).alias('genre'),
            published.alias('published'),               # for people, as written in the article
            *published_range(published),                # for range queries, see published_range
            clean_plot(F.regexp_extract('value', PLOT_RE, 2)).alias('plot'),
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
            # the crawled date is a single day ('2010-09-20'), so the range starts and ends on it;
            # try_ because Spark 4 runs in ANSI mode, where a plain cast would fail the whole job on a bad value
            F.col("machine_release_date").try_cast('date').alias("crawled_release_from"),
            F.col("machine_release_date").try_cast('date').alias("crawled_release_to"),
            F.trim(F.regexp_replace("abstract", AUTO_SUMMARY_RE, '')).alias("crawled_abstract"),
        )
    )


    # crawled.show(30, truncate=50)
    # books.show(30, truncate=50)

    joined = crawled.join(books, on='title_key', how='outer')

    # joined.show(30, truncate=50)

    documents = transform_for_lucene(merge_sources(joined))
    save_single_json(documents, JOIN_JSON)
    # debugging only: a 2nd write runs the whole job (reading the dumps) again, cache documents first if both are on
    # save_single_csv_debug(documents, JOIN_JSON.replace('.jsonl', '_debug.csv'))

    spark.stop()


if __name__ == '__main__':
    main()
