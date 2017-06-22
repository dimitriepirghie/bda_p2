from pyspark import SparkConf, SparkContext, HiveContext

__author__ = 'Pirghie Dimitrie'

conf = SparkConf().setAppName("Preprocessing")
sc = SparkContext(conf=conf)
hc = HiveContext(sc)

query = '''select ArticleID1, ArticleID2, count(*) as CommonSentences
from (
	SELECT S1.ArticleID AS ArticleID1, S2.ArticleID AS ArticleID2
	FROM articles_curated AS S1
	INNER JOIN articles_curated AS S2
	ON S1.preposition_curated_hash=S2.preposition_curated_hash AND S1.ArticleID <> S2.ArticleID
	) AS Joined
GROUP BY ArticleID1, ArticleID2
ORDER BY CommonSentences DESC
LIMIT 20'''

results = hc.sql(query)
results.show()