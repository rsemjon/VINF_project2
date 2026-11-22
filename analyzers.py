
from java.util import HashMap
from org.apache.lucene.analysis.en import EnglishAnalyzer
from org.apache.lucene.analysis.miscellaneous import PerFieldAnalyzerWrapper
from org.apache.lucene.analysis.standard import StandardAnalyzer

ENGLISH_FIELDS = ['plot', 'abstract', 'subjects', 'categories']


def build_analyzer():
    english = EnglishAnalyzer()
    per_field = HashMap()
    for field in ENGLISH_FIELDS:
        per_field.put(field, english)
    return PerFieldAnalyzerWrapper(StandardAnalyzer(), per_field)
