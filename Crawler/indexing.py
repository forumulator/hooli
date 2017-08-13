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
    doc_content_dict = hbase_util.retrieve_docs_html(num_docs_to_index)
    if len(doc_content_dict) == 0:
      break

    t1 = time.time()
    for doc in doc_content_dict.keys():
      
      # remove this line for next crawling
      # doc_content_dict[doc] = re.sub('[\W_]+', ' ', doc_content_dict[doc])
      # Need to do stemming and stop word removal
      doc_content_dict[doc] = doc_content_dict[doc].lower().split()
      doc_content_dict[doc] = index_one_file(doc_content_dict[doc])
      hbase_util.update_inv_index(doc, doc_content_dict[doc])
    logging.info("Completed indexing %s docs in %f sec" %(num_docs_to_index,
     time.time() - t1))



def calc_weights():
  """
  Makes a 
  """

if __name__ == "__main__":
  logger.initialize()
  build_index()
