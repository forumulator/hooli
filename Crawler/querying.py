# Code for ranking documents based on query
import logging
import math
import re
import time

import hbase_util
import logger

from index_mgr_obj import index_mgr

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
    for tf, no_docs in results[doc]:
      score += tf*(math.log(corpus_sz/no_docs))
    results_rank.append((score, doc))
  results_rank = sorted(results_rank, reverse=True)
  logging.info("Scores after ranking: %s" %results_rank[:10])
  return [doc for sc, doc in results_rank]

class Bm25Ranker:
  # bm parameters, can be adjusted
  bm_b = 0.75
  bm_k = 1.6
  avg_doc_len = index_mgr.p_avg_doc_len

  # query needs to be the sanitized query
  def __init__(self, query, tfidf = None):
    self.words = query.split()
    self.tfidf = tfidf

  def preprocess(self):
    if not (self.tfidf is None):
      return
    self.tfidf = {}
    for term in self.words:
      compute_tf_idf(term, self.tfidf)

  def idf_bm(self, containing):
    return math.log((corpus_sz - containing + 0.5) \
        / (containing + 0.5))

  def bm25_term_score(self, doc, tf, containing):
    doc_len = hbase_util.get_doc_length(doc)
    return (self.idf_bm(containing) * ((tf * (bm_k + 1)) \
        / (tf + (bm_k * (1 - bm_b +  \
          (bm_b * (doc_len) / (avg_doc_len)))))))

  def rank(self):
    self.preprocess()
    
    tfidf = self.tfidf
    results = [] # Contains all doc scores
    for doc in tfidf.keys():
      score = 0
      # Doc score is the sum of term scores
      # for each query term
      for term in tfidf[doc]:
        score += self.bm25_term_score(doc, term[0], term[1])
      
      results.append((score, doc))
    # Sort by descending order of score and return 
    results = sorted(results, reverse = True)
    return [doc for sc, doc in results]



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