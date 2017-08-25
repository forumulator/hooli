# Code for indexing
import logging
import random
import re
import time

import hbase_util
from index_mgr_obj import index_mgr

import logger

from nltk.stem import WordNetLemmatizer
from nltk.corpus import stopwords

from functools import lru_cache


class PreProcessor:
""" Word list preprocessor, both for indexing and querying.
Currently removes stop words and lemmatizes all words
"""

  def __init__(self):
    self.wnl = WordNetLemmatizer()
    self.stopwords = set(stopwords.words('english'))

  # 50 kb lemmatizer cache
  @lru_cache(maxsize = 50000)
  def lemmatize(self, word):
    return self.wnl.lemmatize(word)

  def remove_stop_words(self):
    pos_dict = self.pos_dict
    all_words = set(pos_dict.keys())
    rel_words = all_words - self.stopwords

    removed = all_words - rel_words
    for rem_word in removed:
      pos_dict[word][1] = ''

  def process(self, word_list):
    """ Preprocess word_list inplace currently
    """
    pos_dict = {}
    for pos, word in enumerate(word_list):
      if not word in pos_dict:
        pos_dict[word] = [[], word]
      pos_dict[word][0].append(pos)

    self.pos_dict = pos_dict
    self.remove_stop_words()

    for word in pos_dict:
      if pos_dict[word][1] == '':
        continue
      pos_dict[word][1] = self.lemmatize(word)

    # proc_list = [None] * len(word_list)
    for word in pos_dict:
      proc_word = pos_dict[word][1]
      for pos in pos_dict[word][0]:
        word_list[pos] = proc_word

    return True

  def process_small_list(self, word_list):
    """ Inplace processing of small word list
    """
    for pos, word in enumerate(word_list):
      if word in self.stopwords:
        word_list[pos] = ''
      else:
        word_list[pos] = self.lemmatize(word)
 


class Indexer:
  num_docs_to_index = 30

  def __init__(self, index_mgr, hbase_util):
    self.index_mgr = index_mgr
    self.hbase_util = hbase_util
    # count of total pages indexed
    self.indexed_page_count = 0
    self.pre_processor = PreProcessor()

    self.inv_index = {}
    logger.initialize()


  def index_one_file(self, doc, termlist):
    fileIndex = {}
    tf = {}
    for index, word in enumerate(termlist):
      if word in fileIndex.keys():
        tf[word] += 1
        fileIndex[word] += "%s " %index
      else:
        tf[word] = 1
        fileIndex[word] = "%s " %index

    for word in fileIndex:
      if not word in self.inv_index:
        self.inv_index[word] = {}
      # Add this docs index
      self.inv_index[word][doc] = fileIndex[word]

    return max(tf.values())


  def index_batch(self, doc_content_dict):
    """ Index batch of documents whose content is given in
    doc_content_dict
    """
    added_length = 0
    self.inv_index, md = {}, {}
    # index all docs
    for doc in doc_content_dict.keys():  
      doc_content_dict[doc] = doc_content_dict[doc].lower().split()
      self.pre_processor.process(doc_content_dict[doc])

      doc_len = len(doc_content_dict[doc])
      added_length += doc_len
      # Create inverted index for file
      md[doc] = (doc_len, self.index_one_file(doc, doc_content_dict[doc]))

    hbase_util.update_inv_index_batch(self.inv_index, md)
    index_mgr.update_tot_doc_len(len(added_length))


  def build_index(self):
    index_mgr = self.index_mgr
    hbase_util = self.hbase_util

    while(True):
      num_docs, start_id = index_mgr.retrieve_docs_ids(Indexer.num_docs_to_index)
      self.indexed_page_count += num_docs

      if num_docs == 0:
        if index_mgr.is_crawling_done():
          break
        else:
          logging.info("Nothing to index, sleeping for 5")
          time.sleep(5)
          continue

      doc_content_dict = hbase_util.retrieve_docs_content(start_id, num_docs)

      t1 = time.time()
      self.index_batch(doc_content_dict)
      logging.info("Completed indexing %s docs in %f sec" %(Indexer.num_docs_to_index,
          time.time() - t1))

    return self.indexed_page_count



def calc_weights():
  """
    Makes a second pass to calculate the bm25 and tf-idf scores 
    """
    # --------------------------------------------
    # Need to make this multi-threaded
    # There's a huge computation network trade-off
    # --------------------------------------------
    corpus_sz = hbase_util.get_indexed_corpus_size()
    avg_len = hbase_util.get_corpus_term_sz()//corpus_sz
    # params for bm25
    k1 = 1.5
    b = 0.75
    while(True):
      no_terms = 0
      inv_index_dict, flag = hbase_util.retrieve_inv_index(1)
      if flag == False:
        break
      t1 = time.time()
      for term, row in inv_index_dict:
        no_terms += 1
        no_docs = len(row)
        for doc in row.keys():
          tf = len(row[doc].split())
          tf_idf = tf*(math.log(corpus_sz/no_docs))
          # calculating tf_idf_norm
          max_tf = hbase_util.get_max_tf(doc.decode("utf-8")[5:])
          final_tf = 0.5 + 0.5*(tf/max_tf)
          tf_idf_n = final_tf*(math.log((corpus_sz - no_docs)/no_docs))
          # calculating bm25
          doc_len = hbase_util.get_doc_length(doc.decode("utf-8")[5:])
          final_tf = (tf *(k1+1))/(tf + k1*(1-b+b*(doc_len/avg_len)))
          bm25 = final_tf*(math.log((corpus_sz - no_docs + 0.5)/(no_docs + 0.5)))
          # appending to the string
          init_string = row[doc].decode("utf-8")
          init_string += "\n%s\n%s\n%s" %(tf_idf, tf_idf_n, bm25)
          row[doc] = bytes(init_string, "utf-8")
        hbase_util.update_inv_index_score(term, row)
      logging.info("Completed updating the score for %s terms in %f sec" %(no_terms,
       time.time() - t1))

# TODO: Create the indexer object in a thread
if __name__ == "__main__":
  indxr = Indexer(index_mgr, hbase_util)
  pages = indxr.build_index()
  print("Indexer done after indexing %d pages" % pages)
  # No need to use index manager for this one
  # indxr.calc_weights()