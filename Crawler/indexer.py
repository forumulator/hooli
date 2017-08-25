# Code for indexing
import logging
import random
import re
import time

import hbase_util
from index_mgr_obj import index_mgr

import logger


class Indexer:
  num_docs_to_index = 30

  def __init__(self, index_mgr, hbase_util):
    self.index_mgr = index_mgr
    self.hbase_util = hbase_util
    # count of total pages indexed
    self.indexed_page_count = 0
    logger.initialize()

    def index_one_file(termlist):
      fileIndex = {}
      tf = {}
      for index, word in enumerate(termlist):
        if word in fileIndex.keys():
          tf[word] += 1
          fileIndex[word] += "%s " %index
        else:
          tf[word] = 1
          fileIndex[word] = "%s " %index
      return (fileIndex, max(tf.values()))


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
      for doc in doc_content_dict.keys():  
        doc_content_dict[doc] = doc_content_dict[doc].lower().split()
        doc_content_dict[doc], max_tf = self.index_one_file(doc_content_dict[doc])
        hbase_util.update_inv_index(doc, doc_content_dict[doc], max_tf)

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