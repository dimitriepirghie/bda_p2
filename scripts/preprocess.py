import time
import xml.etree.ElementTree as ET

from StringIO import StringIO
from pyspark import SparkConf, SparkContext, HiveContext
from multiprocessing.pool import ThreadPool
from pyspark.sql.types import StructType, StructField, StringType

__author__ = 'Pirghie Dimitrie'

conf = SparkConf().setAppName("Preprocessing")
sc = SparkContext(conf=conf)
hc = HiveContext(sc)

# Read Files Content
files = sc.wholeTextFiles('gs://bda-p2-ddc/pmc_oa_2/1000/*/records*.xml.gz', use_unicode=True, minPartitions=256);


def build_xml_object(string):
    it = ET.iterparse(StringIO(string))
    for _, el in it:
        if '}' in el.tag:
            el.tag = el.tag.split('}', 1)[1]  # strip all namespaces
    return it.root


def extract_test_from_p_element(p_element):
    text = ''.join(list(p_element.itertext()));

    text = text.strip()
    text_len = len(text)

    if text_len > 0:
        if text[text_len - 1] not in ['.', ';', '!', '?']:
            text += '. '

    return text + '\n'


def extract_test_from_sec_element(sec_element):
    text = ''

    for child in sec_element:
        if child.tag == 'p':
            text += extract_test_from_p_element(child)
        elif child.tag == 'sec':
            text = text + extract_test_from_sec_element(child)
    return text + '\n'


def extract_text_from_body_element(body_element):
    try:
        root_secs = body_element.findall('sec')

        text = ''
        for sec in root_secs:
            text += extract_test_from_sec_element(sec)
        return text
    except Exception as e:
        return ''


def extract_information_from_article(article):
    filename = article[0]
    article_xml_root = article[1]

    id = article_xml_root.find('front/article-meta/article-id[@pub-id-type="pmcid"]').text

    abstract = ''
    try:
        abstract = ''.join(list(article_xml_root.find('front/article-meta/abstract').itertext()))
    except:
        abstract = ''

    text = extract_text_from_body_element(article_xml_root.find('body'))

    return filename, id, abstract.strip().replace('\n', ' '), text.strip().replace('\n', ' '), ''


def extract_arcticles(xml_file):
    article_roots = xml_file[1].findall('ListRecords/record/metadata/article')

    args = [(xml_file[0], articleRoot) for articleRoot in article_roots]

    pool = ThreadPool(8)
    results = [row for row in pool.map(extract_information_from_article, args)]
    pool.close()

    return results

start = time.time()

# Parse as XML objects
xmlFiles = files.map(lambda file: (file[0], build_xml_object(file[1].encode('utf-8'))))

# Extract article XML nodes
articles = xmlFiles.flatMap(lambda xml_file: [(xml_file[0], articleRoot) for articleRoot in xml_file[1].findall('ListRecords/record/metadata/article')])

# Extract id, abstract, text from articles
rows = articles.map(extract_information_from_article)

schema = StructType([
    StructField('File', StringType(), True),
    StructField('ArticleId', StringType(), True),
    StructField('ArticleAbstract', StringType(), True),
    StructField('ArticleText', StringType(), True)
])

hc.createDataFrame(rows, schema).write.mode("append").saveAsTable('articles')

end = time.time()

print('Elapsed time:')
print(end - start)

sc.stop()
