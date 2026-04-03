#!/usr/bin/env python3
import sys

current_term = None
postings = []

def flush(term, postings):
    print(f"{term}\t{len(postings)}\t{','.join(postings)}")

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.split('\t')
    if len(parts) != 4:
        continue
    term, doc_id, tf, dl = parts
    if term != current_term:
        if current_term is not None:
            flush(current_term, postings)
        current_term = term
        postings = []
    postings.append(f"{doc_id}:{tf}:{dl}")

if current_term is not None:
    flush(current_term, postings)
