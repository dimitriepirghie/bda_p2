from pyspark import SparkConf, SparkContext, HiveContext
from pyspark.sql.types import StructType, StructField, StringType
import unicodedata
from nltk.tokenize.punkt import PunktParameters
from nltk.corpus import stopwords
import nltk
import string

__author__ = 'Pirghie Dimitrie'

conf = SparkConf().setAppName("Preprocessing")
sc = SparkContext(conf=conf)
hc = HiveContext(sc)


punkt_param = PunktParameters()
abbreviations = ['al']
punkt_param.abbrev_types = set(abbreviations)
tokenizer = nltk.PunktSentenceTokenizer(punkt_param)
stemmer = nltk.SnowballStemmer("english")
stop_words = set(stopwords.words('english'))
punctuationSymbols = list(string.punctuation)
replacePunctuation = string.maketrans(string.punctuation, ' ' * len(string.punctuation))


scheme = StructType([
    StructField('articleid', StringType(), True),
    StructField('preposition_curated', StringType(), True),
    StructField('preposition_curated_hash', StringType(), True)
])


def extract_essential_words(unicode_sentence, recombine_preposition=False):
    words = unicodedata.normalize('NFKD', unicode_sentence).encode('ascii','ignore').translate(replacePunctuation).strip().lower().split()
    words = filter(lambda w: len(w) > 3, words)
    words = filter(lambda w: w not in stop_words, [stemmer.stem(w) for w in words])
    return ' '.join(words) if recombine_preposition else words

data_query = hc.sql('SELECT articleid, articleabstract, articletext FROM articles LIMIT 2000')
# for x in data_query.rdd.collect():
#    print x
# Combine article abstract and article context into a list of statements.
data = data_query.rdd.map(lambda row: (row['articleid'], tokenizer.tokenize(row['articleabstract'].strip() + row['articletext'].strip())))
#for x in data.collect():
#    print x

# For each statement curate and remember only the essentials words and parts of words
data = data.map(lambda article: (article[0], [extract_essential_words(sentence, True) for sentence in article[1]]))
#for x in data.collect():
#    print x

# For each statement build a new data structure (ArticleId, Sentence, Hash(Statement)
data = data.map(lambda article: ([(article[0], sentence, hash(sentence)) for sentence in article[1]]))
data_l = data.collect()

data_all = []
for entry in data_l:
    for preposition in entry:
        data_all.append(preposition)

# Store new data using hive!
hc.createDataFrame(data_all, scheme).write.mode("append").saveAsTable('articles_curated')

#save as a new hive table
#hiveContext.createDataFrame(data, scheme).write.mode("append").saveAsTable('articles_curated')

sc.stop()