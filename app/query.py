import sys
import re
import math
from pyspark import SparkContext, SparkConf
from cassandra.cluster import Cluster

SCYLLA_HOST = 'cassandra-server'
KEYSPACE = 'search_engine'
K1 = 1.0
B = 0.75

query_str = " ".join(sys.argv[1:])
query_terms = re.findall(r'[a-zA-Z]+', query_str.lower())

cluster = Cluster([SCYLLA_HOST])
session = cluster.connect(KEYSPACE)

global_stats = {row.key: row.value for row in session.execute("SELECT key, value FROM global_stats")}
N = global_stats.get('N', 0)
dl_avg = global_stats.get('dl_avg', 1.0)

doc_stats = {row.doc_id: (row.title, row.dl) for row in session.execute("SELECT doc_id, title, dl FROM doc_stats")}

postings_data = []
term_df = {}
for term in set(query_terms):
    rows = list(session.execute("SELECT doc_id, tf FROM inverted_index WHERE term = %s", (term,)))
    term_df[term] = len(rows)
    for row in rows:
        postings_data.append((term, row.doc_id, row.tf))

cluster.shutdown()

conf = SparkConf().setAppName("BM25Search")
sc = SparkContext(conf=conf)

b_N = sc.broadcast(N)
b_dl_avg = sc.broadcast(dl_avg)
b_term_df = sc.broadcast(term_df)
b_doc_stats = sc.broadcast(doc_stats)

def compute_bm25(record):
    term, doc_id, tf = record
    df = b_term_df.value.get(term, 1)
    dl = b_doc_stats.value.get(doc_id, ("", 1))[1]
    idf = math.log(b_N.value / df)
    tf_norm = (K1 + 1) * tf / (K1 * ((1 - B) + B * dl / b_dl_avg.value) + tf)
    return (doc_id, idf * tf_norm)

results = (
    sc.parallelize(postings_data)
    .map(compute_bm25)
    .reduceByKey(lambda a, b: a + b)
    .sortBy(lambda x: -x[1])
    .take(10)
)

print(f"\nTop 10 results for query: '{query_str}'")
for rank, (doc_id, score) in enumerate(results, 1):
    title = doc_stats.get(doc_id, ("Unknown", 0))[0]
    print(f"{rank}. [Doc {doc_id}] {title} (score: {score:.4f})")

sc.stop()
