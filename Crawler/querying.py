# Code for ranking documents based on query
import logging
import math
import re
import time

import hbase_util
import logger

# Get actual value from Hbase
corpus_sz = hbase_util.get_indexed_corpus_size()
print(corpus_sz)
def sanitize_query(query):
  pattern = re.compile('[\W_]+')
  query = pattern.sub(' ',query).lower()
  # Remove stop words and convert stemming
  return query

def compute_tf_idf(term, results, operator="or"):
  doc_list = hbase_util.get_occuring_docs(term)
  no_docs = len(doc_list)
  for doc in doc_list:
    tf = len(doc_list[doc].split())
    if doc in results.keys():
      results[doc].append((tf, no_docs))
    else:
      results[doc] = [(tf, no_docs)]

def tf_idf1(results):
  """
  Calculates score of each doc as SUM over each term -
  term_freq(term)*log(size_corpus/no_of_docs_term_occurs_in) 
  """
  results_rank = []
  # print(results)
  for doc in results.keys():
    score = 0
    for tf,no_docs in results[doc]:
      score += tf*(math.log(corpus_sz/no_docs))
    results_rank.append((score, doc))
  results_rank = sorted(results_rank, reverse=True)
  logging.info("Scores after ranking: %s" %results_rank[:10])
  return [doc for sc,doc in results_rank]


rank_fn = {"tf-idf1": tf_idf1}
def rank_results(query, rank_name="tf-idf1"):
  # Add boolean operators
  logger.initialize()
  t1 = time.time()
  results = {}
  query = sanitize_query(query)
  for term in query.split():
    compute_tf_idf(term, results)

  results_rank = rank_fn[rank_name](results)[:10]
  # print(results_rank[:10])
  url_list = hbase_util.get_url(results_rank)
  logging.info("Ranked the results for the query: %s in %f sec" %(query,
    time.time()-t1))
  for url in url_list:
    print(url)

if __name__ == "__main__":
  query = input("Search: ")
  rank_results(query)