# Code for indexing
import logging
import random
import re
import time

import hbase_util
import logger


num_docs_to_index = 30

def index_one_file(termlist):
  fileIndex = {}
  for index, word in enumerate(termlist):
    if word in fileIndex.keys():
      fileIndex[word] += "%s " %index
    else:
      fileIndex[word] = "%s " %index
  return fileIndex

def build_index():
  while(True):
    num_docs, start_id = index_mgr.retrieve_docs_ids(num_docs_to_index)
    if num_docs == 0:
      # TODO: Sleep instead of break?
      break

    doc_content_dict = hbase_util.retrieve_docs_content(start_id, num_docs)

    for doc in doc_content_dict.keys():
      t1 = time.time()
      # remove this line for next crawling
      # doc_content_dict[doc] = re.sub('[\W_]+', ' ', doc_content_dict[doc])
      # Need to do stemming and stop word removal
      doc_content_dict[doc] = doc_content_dict[doc].lower().split()
      doc_content_dict[doc] = index_one_file(doc_content_dict[doc])
      hbase_util.update_inv_index(doc, doc_content_dict[doc])
      index_mgr.update_avg_doc_len(len(doc_content_dict[doc]))
      logging.info("Completed indexing doc: %s in %f sec" %(doc,
        time.time() - t1))



def calc_weights():
  """
  Makes a 
  """

if __name__ == "__main__":
  logger.initialize()
  build_index()
