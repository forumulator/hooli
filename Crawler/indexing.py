# Code for indexing
import logging
import re
import time

import hbase_util
import logger

num_docs_to_index = 30

def index_one_file(termlist):
  fileIndex = {}
  for index, word in enumerate(termlist):
    if word in fileIndex.keys():
      fileIndex[word].append(index)
    else:
      fileIndex[word] = [index]
  return fileIndex

def build_index():
  count = 0
  while(True):
    doc_content_dict = hbase_util.retrieve_docs_html(num_docs_to_index)
    if len(doc_content_dict) == 0:
      break

    for doc in doc_content_dict.keys():
      t1 = time.time()
      # remove this line for next crawling
      doc_content_dict[doc] = re.sub('[\W_]+', ' ', doc_content_dict[doc])
      doc_content_dict[doc] = doc_content_dict[doc].split()
      doc_content_dict[doc] = index_one_file(doc_content_dict[doc])
      hbase_util.update_inv_index(doc, doc_content_dict[doc])
      count+=1
      print(count)
      logging.info("Completed indexing doc: %s in %f sec" %(doc,
        time.time() - t1))

if __name__ == "__main__":
  logger.initialize()
  build_index()
