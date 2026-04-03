#!/usr/bin/env python3
import sys
import re
import os
from collections import Counter

filename = os.environ.get('mapreduce_map_input_file', '')
doc_id = os.path.basename(filename).split('_', 1)[0]

text = sys.stdin.read()
words = re.findall(r'[a-zA-Z]+', text.lower())
word_counter = Counter(words)
dl = sum(word_counter.values())

for term, tf in word_counter.items():
    print(f"{term}\t{doc_id}\t{tf}\t{dl}")
