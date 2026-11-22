import argparse
import html
import os
import re
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


SPARK_REMOTE = "sc://localhost:15002"   # Spark Connect port exposed by docker-compose

# paths as seen by the Spark container (see docker-compose volumes)
CONTAINER_INPUT_DIR = "/data"        # <- ../crawler_data
CONTAINER_OUTPUT_DIR = "/output"     # <- ./data

OUTPUT_FIELDS = [
    'title', 'author', 'reading_level', 'language',
    'subjects', 'release_date', 'machine_release_date', 'abstract', 'website', 'extracted_at',
    'source_file_path',
]

RESULT_SCHEMA = StructType([
    StructField('title', StringType()),
    StructField('author', StringType()),
    StructField('reading_level', DoubleType()),
    StructField('language', StringType()),
    StructField('subjects', StringType()),
    StructField('release_date', StringType()),
    StructField('abstract', StringType()),
])


class Extractor:

    FIELD_TEMPLATE = r'<th>\s*{label}\s*</th>\s*<td[^>]*>(.*?)</td>'

    def __init__(self, html_text: str):
        self.html = html_text


    def _clean(self, raw: str) -> str:
        text = re.sub(r'<br\s*/?>', ' ', raw, flags=re.I)
        text = re.sub(r'<[^>]+>', '', text)
        text = html.unescape(text)
        return re.sub(r'\s+', ' ', text).strip()

    def _field(self, label: str) -> str | None:
        pattern = self.FIELD_TEMPLATE.format(label=re.escape(label))
        match = re.search(pattern, self.html, re.I | re.S)
        return self._clean(match.group(1)) if match else None

    def _all_fields(self, label: str) -> list[str]:
        pattern = self.FIELD_TEMPLATE.format(label=re.escape(label))
        matches = re.findall(pattern, self.html, re.I | re.S)
        return [self._clean(m) for m in matches]


    def get_title(self) -> str | None:
        return self._field('Title')

    def get_author(self) -> str | None:
        author = self._field("Author")

        if author is None:
            return None

        # strip trailing life years / date range, 1-4 digits each (e.g. ", 1865-1952", ", 553-599")
        author = re.sub(r",\s*\d{1,4}(?:\s*-\s*\d{0,4})?\s*$", "", author).strip()

        return author

    def get_language(self) -> str | None:
        return self._field('Language')

    def get_release_date(self) -> str | None:
        return self._field('Release Date')

    def get_reading_level(self) -> float | None:
        raw = self._field('Reading Level')
        if not raw:
            return None
        match = re.search(r'([\d]+(?:\.[\d]+)?)', raw)
        return float(match.group(1)) if match else None

    def get_subjects(self) -> list[str]:
        return self._all_fields('Subject')

    def get_abstract(self) -> str:
        pattern = r'<span\b[^>]*class=["\']readmore-container["\'][^>]*>(.*?)</span>'
        match = re.search(pattern, self.html, flags=re.DOTALL | re.IGNORECASE)

        if not match:
            return ""

        return self._clean(match.group(1))


    def extract(self) -> dict:
        return {
            'title': self.get_title(),
            'author': self.get_author(),
            'reading_level': self.get_reading_level(),
            'language': self.get_language(),
            'subjects': self.get_subjects(),
            'release_date': self.get_release_date(),
            'abstract': self.get_abstract(),
        }

    def clean_value(self, value: str | None) -> str:
        if value is None:
            return ""

        text = str(value)
        text = html.unescape(text)
        text = re.sub(r"\s+", " ", text)
        text = re.sub(r"['’]s\b", "", text, flags=re.IGNORECASE)
        tokens = re.findall(r"[A-Za-z0-9]+(?:[-.'’][A-Za-z0-9]+)*", text)

        return " ".join(tokens)


    def clean_record(self, data: dict) -> dict:
        return {
            "title": self.clean_value(data.get("title")),
            "author": self.clean_value(data.get("author")),
            "reading_level": self.clean_value(data.get("reading_level")),
            "language": self.clean_value(data.get("language")),
            "subjects": self.clean_value("; ".join(data.get("subjects", []))),
            "release_date": self.clean_value(data.get("release_date")),
            "abstract": self.clean_value(data.get("abstract")),
        }


# ----------------------------------------------------------------------------
# Spark part
# ----------------------------------------------------------------------------

def extract_file(file_path: str) -> dict | None:
    """Runs on the executors: read one html file and extract the fields (same as the original per-row logic)."""
    if not file_path or not os.path.exists(file_path):
        return None

    with open(file_path, encoding='utf-8', errors='ignore') as f_html:
        html_text = f_html.read()

    data = Extractor(html_text).extract()

    print(data['title'])
    return {
        'title': data['title'],
        'author': data['author'],
        'reading_level': data['reading_level'],
        'language': data['language'],
        'subjects': '; '.join(data['subjects']),
        'release_date': data['release_date'],
        'abstract': data['abstract'],
    }


def extract_informations(spark, input_tsv: str, output_dir: str) -> None:
    extract_udf = F.udf(extract_file, RESULT_SCHEMA)

    visited = spark.read.csv(input_tsv, sep='\t', header=True)

    # windows path from crawler ("crawler_data\pages\x.html") -> path inside the container
    container_path = F.regexp_replace(
        F.regexp_replace(F.col('file_path'), r'^crawler_data[\\/]', CONTAINER_INPUT_DIR + '/'),
        r'\\', '/',
    )

    df = (
        visited
        .withColumn('source_file_path', F.col('file_path'))
        .withColumn('data', extract_udf(container_path))
        .where(F.col('data').isNotNull())          # skip missing files
        .select(
            'data.*',
            # 'Sep 20, 2010' -> 2010-09-20 (written as yyyy-MM-dd), null when it does not parse;
            # try_ because Spark 4 runs in ANSI mode, where a plain to_date would fail the whole job
            F.to_date(F.try_to_timestamp('data.release_date', F.lit('MMM d, yyyy'))).alias('machine_release_date'),
            F.col('url').alias('website'),
            F.lit(datetime.now(timezone.utc).isoformat()).alias('extracted_at'),
            'source_file_path',
        )
        .select(*OUTPUT_FIELDS)
    )

    (
        df.coalesce(1)
        .write.mode('overwrite')
        .option('header', True)
        .option('sep', '\t')
        .option('quote', '"')
        .option('escape', '"')
        .csv(output_dir)
    )


def main():
    spark = SparkSession.builder.remote(SPARK_REMOTE).getOrCreate()
    # ship this module to  Spark server so the UDF can import Extractor
    spark.addArtifacts(os.path.abspath(__file__), pyfile=True)

    extract_informations(spark, f'{CONTAINER_INPUT_DIR}/visited_finalizing.tsv', f'{CONTAINER_OUTPUT_DIR}/books_extracted')
    spark.stop()


if __name__ == '__main__':
    main()
