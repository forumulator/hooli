# 
# Author: s.abhiram
#
# This file contains code for creating rows and querying the tables of Hbase.
#
import Pyro4
import happybase
import logging
import sys
import time
import traceback

import logger
from config import *

def connect_to_index_mgr(uri = None):
  if uri is None:
    uri = input("Enter the uri of the index manager: ").strip()
  index_mgr = Pyro4.Proxy(uri)
  if index_mgr is None:
    logging.error("Couldn't connect to IndexManager with URI: %s" %uri)
    raise RuntimeError("Couldn't connect to IndexManager with URI: %s" %uri)

  return index_mgr

# Global index_mgr object, to update
# the crawled doc count
index_mgr = connect_to_index_mgr()

def get_hbase_connection():
  return happybase.Connection(host=thrift_host ,port=thrift_port)


def update_crawl_count(count):
  '''
  Updates the Hbase crawl count by count and returns the new value.
  '''
  # Update to index_mgr
  t1 = time.time()
  new_count = index_mgr.add_crawl_count(count)
  logging.info("Updated crawl_count in index manager in %s sec"
    %(time.time() - t1))

  # Update in Hbase crawl_stats table
  t1 = time.time()
  con = get_hbase_connection()
  table = con.table('crawl_statistics')
  db_count = table.counter_inc(b'crawl', b'stats:count', value=count)
  logging.info("Updated hbase crawl count to %s in %s sec" %(db_count,
      time.time() - t1))

  return int(new_count)


def post_web_doc(url_hash, url, html_string):
  '''
  Created a new record in the database for the given entry.
  '''
  t1 = time.time()
  con = get_hbase_connection()
  table = con.table('web_doc')
  table.put(bytes(url_hash, "utf-8"), 
    {
      b'details:url' : bytes(url, "utf-8"),
      b'details:html': bytes(html_string, "utf-8"),
    })

  con = get_hbase_connection()
  table = con.table('enumerate_docs')
  num_id = table.counter_inc(b'gen_next_id', b'cf1:counter')
  table.put(bytes(str(num_id), "utf-8"),
    {
      b'cf1:urlhash': bytes(url_hash, "utf-8")
    })
  logging.info("Stored the web page in the database in %s sec." %(time.time() - t1))


def check_web_doc(url_hash):
  '''
  Checks if the given url hash is already present in the database.
  Args: urlhash - <str> denoting the id of row to search.
  Returns True if there exists a row with the given id whose
  html_ field timestamp is less than a week old, False otherwise.
  '''
  t1 = time.time()
  con = get_hbase_connection()
  table = con.table('web_doc')
  row = table.row(bytes(url_hash, "utf-8"), include_timestamp=True)
  logging.info("Checked the database in %s sec."%(time.time() - t1))
  if not row:
    return False
  else:
    try:
      val, timestamp = row[b'details:html']
      sev_days_to_sec = 86400*7
      if timestamp is not None and (t1-timestamp)/sev_days_to_sec > 1:
        return False

    except Exception as e:
      # Expecting error if row doesn;t have the given field
      tb = traceback.format_exc()
      logging.error("Problem %s:\n%s" %(e, tb))
      return False

  logging.info("Given urlhash is already present in the database.")
  return True


def update_inv_index(url_hash, doc_index):
  """
  Given doc identified by its url_hash and a dict - doc_index
  which contains terms of the doc as keys and the list of the term's
  positions in the doc as values, updates the inv_index and the web_doc table.
  """
  t1 = time.time()
  term_count = len(doc_index)
  con = get_hbase_connection()
  table = con.table('web_doc')
  # |D| is needed for bm25 calculation
  table.put(bytes(url_hash, "utf-8"),
    {
      b'details:term_count' : bytes(str(term_count), "utf-8")
    })
  # storing total term count to calculate average doc_length
  table = con.table("index_stats")
  table.counter_inc(b'index', b'stats:total_length', value=term_count)

  con = get_hbase_connection()
  table = con.table('inv_index')
  bat = table.batch()
  # TODO - try changing to with
  col_name = "docs:%s" %url_hash
  for term in doc_index:
    bat.put(bytes(term, "utf-8"),
      {
      bytes(col_name, "utf-8") : bytes(doc_index[term], "utf-8")
      })
    # I can do this in another pass
    #table.counter_inc(bytes(term, "utf-8"), b'docs:counter')

  try:
    bat.send()
  except Exception as e:
    # It's possible that the connection timed out
    logging.error("Doc with url: %s suffered: %s" %(url_hash, e))

  logging.debug("Updated inv_index for doc: %s in %f time" %(url_hash,
    (time.time() - t1)))


def retrieve_docs_content(start_id, num_docs):
  """
  Given the number of docs to be retrieved, returns a dict
  with the doc identifier - which is its url_hash as key and the
  html text of the doc as the value.
  """
  # TODO - Store the IPs vs ranges of docs indexed by them
  last_id = start_id + num_docs - 1

  import socket
  t1 = time.time()
  con = get_hbase_connection()
  table = con.table('index_stats')

  # Update the table
  table.counter_inc(b'index', b'stats:counter', value = num_docs)
  table.put(bytes(str("%d-%d" %(start_id, last_id)), "utf-8"),
    {
      b'stats:ip' : bytes(socket.gethostname(), "utf-8")
    })

  doc_list = {}
  con = get_hbase_connection()
  tab1 = con.table('enumerate_docs')
  tab2 = con.table('web_doc')
  for doc_id in range(start_id, last_id + 1):
    url_hash = tab1.row(bytes(str(doc_id), "utf-8")).get(b'cf1:urlhash')
    if url_hash is None:
      logging.error("Couldn't retrieve url_hash for doc_id: %d, skipping" % doc_id)
      continue
    # Add doc to doc_list
    content = tab2.row(url_hash)[b'details:html']
    # Key is a 2 tuple of doc_id, url_hash
    doc_list[url_hash.decode("utf-8")] = content.decode("utf-8")

  logging.info("Retrieved docs %d-%d in %f sec" %(start_id, doc_id - 1,
    time.time() - start_time))

  return doc_list


def get_occuring_docs(term):
  """
  Returns a dict of docs where the term occurs
  """
  t1 = time.time()
  con = get_hbase_connection()
  table = con.table('inv_index')
  row = table.row(bytes(term, "utf-8"))

  docs_dict = {}
  for doc_with_cf in row:
    doc_str = doc_with_cf.decode("utf-8")[5:]
    docs_dict[doc_str] = row[doc_with_cf]
  logging.info("Retrived %s docs for term: %s in %f secs" %(len(docs_dict),
    term, time.time()-t1))
  return docs_dict


def remove_table(table_list):
  """
  Disables and deletes the tables given in the list from Hbase.
  """
  con = get_hbase_connection()
  for table in table_list:
    try:
      con.delete_table(table, True)
    except Exception as e:
      tb = traceback.format_exc()
      logging.error("Problem %s:\n%s" %(e, tb))
    else:
      print("Deleted: %s" %table)
      logging.info("Deleted Table: %s" %table)


# TODO: Add cases where Indexers can crash, so
# update the stats:counter after indexing
def get_indexed_corpus_size():
  # Retrieving the index stats:counter becuase that is
  # updated as documents are retrived for indexing
  con = get_hbase_connection()
  table = con.table('index_stats')
  return table.counter_get(b'index', b'stats:counter')


def get_url(doc_list):
  t1 = time.time()
  url_list = []
  for doc in doc_list:
    con = get_hbase_connection()
    table = con.table('web_doc')
    url = table.row(bytes(doc, "utf-8"))[b'details:url']
    url_list.append(url)
  logging.info("Retrived url for given list in %f sec" %(time.time()-t1))
  return url_list


def get_avg_doc_len():
  t1 = time.time()
  conn = get_hbase_connection()
  table = conn.table('index_stats')
  avg_doc_len = table.counter_get(b'index', b'stats:avg_dl')
  logging.info("Retrieved avg doc len in %s secs" \
     %(time.time() - t1))

  return avg_doc_len

# TODO: Do batch updates
def update_avg_doc_len(avg_doc_len):
  """
  Update avg_doc_len
  """
  t1 = time.time()
  conn = get_hbase_connection()
  table = conn.table('index_stats')
  table.counter_set(b'index', b'stats:avg_dl', avg_doc_len)
  logging.info("Updated avg doc len in %s secs" \
     %(time.time() - t1))

  return avg_doc_len

# doc is actually the url_hash
def get_doc_length(doc):
  """ Get the doc lenght of doc with url_hash
  doc
  """
  t1 = time.time()
  con = get_hbase_connection()
  table = con.table('web_doc')
  doc_length = table.row(bytes(doc, "utf-8")) \
      ['details:term_count']
  logging.info("Retrieved doc_length for doc: %s in %s secs" \
     %(doc, time.time() - t1))
  return doc_length

if __name__ == "__main__":
  logger.initialize()