import subprocess
from cassandra.cluster import Cluster

SCYLLA_HOST = 'cassandra-server'
KEYSPACE = 'search_engine'


def read_hdfs_file(path):
    result = subprocess.run(['hdfs', 'dfs', '-cat', path], capture_output=True, text=True)
    return result.stdout.strip().split('\n')


def setup_schema(session):
    session.execute(f"CREATE KEYSPACE IF NOT EXISTS {KEYSPACE} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}")
    session.set_keyspace(KEYSPACE)
    session.execute("DROP TABLE IF EXISTS inverted_index")
    session.execute("DROP TABLE IF EXISTS doc_stats")
    session.execute("DROP TABLE IF EXISTS global_stats")
    session.execute("CREATE TABLE inverted_index (term text, doc_id int, tf int, PRIMARY KEY (term, doc_id))")
    session.execute("CREATE TABLE doc_stats (doc_id int PRIMARY KEY, title text, dl int)")
    session.execute("CREATE TABLE global_stats (key text PRIMARY KEY, value double)")


result = subprocess.run(['hdfs', 'dfs', '-ls', '/data'], capture_output=True, text=True)
doc_titles = {}
for line in result.stdout.strip().split('\n'):
    if '.txt' in line:
        filename = line.strip().split('/')[-1]
        parts = filename.rsplit('.', 1)[0].split('_', 1)
        doc_titles[int(parts[0])] = parts[1].replace('_', ' ') if len(parts) > 1 else ""

cluster = Cluster([SCYLLA_HOST])
session = cluster.connect()
setup_schema(session)

insert_index = session.prepare(f"INSERT INTO {KEYSPACE}.inverted_index (term, doc_id, tf) VALUES (?, ?, ?)")
insert_doc = session.prepare(f"INSERT INTO {KEYSPACE}.doc_stats (doc_id, title, dl) VALUES (?, ?, ?)")
insert_global = session.prepare(f"INSERT INTO {KEYSPACE}.global_stats (key, value) VALUES (?, ?)")

doc_lengths = {}

for line in read_hdfs_file('/indexer/part-00000'):
    if not line.strip():
        continue
    parts = line.split('\t')
    if len(parts) != 3:
        continue
    term, df_str, postings_str = parts
    for posting in postings_str.split(','):
        pparts = posting.split(':')
        if len(pparts) != 3:
            continue
        doc_id, tf, dl = int(pparts[0]), int(pparts[1]), int(pparts[2])
        session.execute(insert_index, (term, doc_id, tf))
        if doc_id not in doc_lengths:
            doc_lengths[doc_id] = dl
            session.execute(insert_doc, (doc_id, doc_titles.get(doc_id, ""), dl))

n = len(doc_lengths)
dl_avg = sum(doc_lengths.values()) / n
session.execute(insert_global, ('N', float(n)))
session.execute(insert_global, ('dl_avg', dl_avg))

print(f"Indexed {n} documents, {sum(1 for _ in doc_lengths)} unique docs, dl_avg={dl_avg:.2f}")
cluster.shutdown()
