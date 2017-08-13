# 
# Author: s.abhiram
#
# This file contains code for creating rows and querying the tables of Hbase.
#

import happybase
import logging
import sys
import time
import traceback

import logger

thrift_port = 9001
thrift_host = "172.16.114.80"

def get_hbase_connection():
  return happybase.Connection(host=thrift_host ,port=thrift_port)

def create_crawl_count_table():
  '''
  Creates all the Hbase tables necessary for crawling,
  storing the web_docs and indexing and initializes them.
  '''
  con = get_hbase_connection()
  
  try:
    # Stores the crawl count which helps signal when to terminate crawling
    if not b'crawl_statistics' in con.tables():
      con.create_table('crawl_statistics',
        {
          'stats': dict(),
        })
      logging.info("Crawl Stats Table created")


    table = con.table('crawl_statistics')
    row = table.row(b'crawl')
    if not row:
      table.counter_set(b'crawl', b'stats:count', 0)
      logging.info("Row crawl created in the table")

    # Stores the web pages crawled and their details
    if not b'web_doc' in con.tables():
      con.create_table('web_doc',
        {
          'details': dict(),
        })
      logging.info("Web docs Table created")

    # Enumerates the crawled pages for indexing
    if not b'enumerate_docs' in con.tables():
      con.create_table('enumerate_docs',
        {
          'cf1': dict(),
        })
      logging.info("Enumerate Docs Table created")

    table = con.table('enumerate_docs')
    row = table.row(b'gen_next_id')
    if not row:
      table.counter_set(b'gen_next_id', b'cf1:counter', 0)
      logging.info("Enumerate Docs counter created")

    # Stores the inverted index
    if not b'inv_index' in con.tables():
      con.create_table('inv_index',
        {
          'docs': dict(),
        })
      logging.info("Inverted Index Table created")

    # Stores the index stats - how many files have been indexed
    if not b'index_stats' in con.tables():
      con.create_table('index_stats',
        {
          'stats': dict(),
        })
      logging.info("Index Stats Table created")

    table = con.table('index_stats')
    table.counter_set(b'index', b'stats:counter', 1)
    table.counter_set(b'index', b'stats:total_length', 0)
    logging.info("Index Stats counter created")

  except Exception as e:
    # Print stack trace
    tb = traceback.format_exc()
    logging.error("Problem %s:\n%s" %(e, tb))
    return False

  return True

def update_crawl_count(count):
  '''
  Updates the Hbase crawl count by count and returns the new value.
  '''
  t1 = time.time()
  con = get_hbase_connection()
  table = con.table('crawl_statistics')
  db_count = table.counter_inc(b'crawl', b'stats:count', value=count)
  logging.info("Updated hbase crawl count to %s in %s sec" %(db_count,
   time.time() - t1))
  return int(db_count)


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

  logging.info("Updated inv_index for doc: %s in %f time" %(url_hash,
    (time.time() - t1)))

def retrieve_docs_html(num_docs):
  """
  Given the number of docs to be retrieved, returns a dict
  with the doc identifier - which is its url_hash as key and the
  html text of the doc as the value.
  """
  # TODO - Store the IPs vs ranges of docs indexed by them
  import socket
  t1 = time.time()
  con = get_hbase_connection()
  table = con.table('index_stats')
  last_id = table.counter_inc(b'index', b'stats:counter', value=num_docs)
  table.put(bytes(str("%d-%d" %(last_id-num_docs+1, last_id)), "utf-8"),
    {
      b'stats:ip' : bytes(socket.gethostname(), "utf-8")
    })

  doc_list = {}
  con = get_hbase_connection()
  tab1 = con.table('enumerate_docs')
  tab2 = con.table('web_doc')
  for doc_id in range(last_id-num_docs+1, last_id+1):
    url_hash = tab1.row(bytes(str(doc_id), "utf-8")).get(b'cf1:urlhash')
    if url_hash is None:
      break
    html_string = tab2.row(url_hash)[b'details:html']
    doc_list[url_hash.decode("utf-8")] = html_string.decode("utf-8")

  logging.info("Retrieved docs %d-%d in %f sec" %(last_id-num_docs, doc_id-1,
    time.time()-t1))
  return doc_list

def get_occuring_docs(term):
  """
  Returns a dict of docs where the term occurs
  """
  con = get_hbase_connection()
  table = con.table('inv_index')
  row = table.row(bytes(term, "utf-8"))

  docs_dict = {}
  for doc_with_cf in row:
    doc_str = doc_with_cf.decode("utf-8")[5:]
    docs_dict[doc_str] = row[doc_with_cf]

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

def get_indexed_corpus_size():
  # Currently returning the number of crawled until index manager is created
  con = get_hbase_connection()
  table = con.table('crawl_statistics')
  return table.counter_get(b'crawl', b'stats:count')

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

if __name__ == "__main__":
  logger.initialize()
  create_crawl_count_table()