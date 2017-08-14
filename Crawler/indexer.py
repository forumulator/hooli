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

  def index_one_file(self, termlist):
    fileIndex = {}
    for index, word in enumerate(termlist):
      if word in fileIndex.keys():
        fileIndex[word] += "%s " %index
      else:
        fileIndex[word] = "%s " %index
    return fileIndex


  def build_index(self):
    index_mgr = self.index_mgr
    hbase_util = self.hbase_util
    while(True):
      num_docs, start_id = index_mgr.retrieve_docs_ids(num_docs_to_index)
      self.indexed_page_count += num_docs
      if num_docs == 0:
        if index_mgr.is_crawling_done():
          break
        else:
          time.sleep(5)

      doc_content_dict = hbase_util.retrieve_docs_content(start_id, num_docs)

      t1 = time.time()
      for doc in doc_content_dict.keys():  
        # remove this line for next crawling
        # doc_content_dict[doc] = re.sub('[\W_]+', ' ', doc_content_dict[doc])
        # Need to do stemming and stop word removal
        doc_content_dict[doc] = doc_content_dict[doc].lower().split()
        doc_content_dict[doc] = index_one_file(doc_content_dict[doc])
        hbase_util.update_inv_index(doc, doc_content_dict[doc])
        index_mgr.update_avg_doc_len(len(doc_content_dict[doc]))

      logging.info("Completed indexing %s docs in %f sec" %(num_docs_to_index,
          time.time() - t1))

    return self.indexed_page_count



def calc_weights():
  """
  Makes a 
  """

# TODO: Create the indexer object in a thread
if __name__ == "__main__":
  pages = Indexer(index_mgr, hbase_util).build_index()
  print("Indexer done after indexing %d pages" % pages)