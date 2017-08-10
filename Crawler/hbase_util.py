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

logging.basicConfig(format='%(asctime)s %(message)s', 
  filename='py_hbase.log',level=logging.DEBUG)

thrift_port = 9001

def get_hbase_connection():
  return happybase.Connection(port=thrift_port)

def create_crawl_count_table():
  '''
  Creates all the Hbase tables and initializes them.
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
      table.counter_inc(b'crawl', b'stats:count')
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
      table.counter_inc(b'gen_next_id', b'cf1:counter')
      logging.info("Enumerate Docs counter created")

    if not b'inv_index' in con.tables():
      con.create_table('inv_index',
        {
          'docs': dict(),
        })
      logging.info("Inverted Index Table created")

  except Exception as e:
    # Print stack trace
    logging.debug("Problem %s" %e)
    traceback.print_exc(file=sys.stdout)
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
      logging.debug("Problem %s" %e)
      traceback.print_exc(file=sys.stdout)
      return False

  logging.info("Given urlhash is already present in the database.")
  return True


def update_inv_index(url_hash, doc_index):
  # If we are not really using the font/formatting of words might as well
  # store just words and their pos in the html_string of web_docs table
  # decreasing its size.
  term_count = len(doc_index)
  con = get_hbase_connection()
  table = con.table('web_doc')
  table.put(bytes(url_hash, "utf-8"),
    {
      b'details:term_count' : bytes(str(term_count), "utf-8")
    })

  con = get_hbase_connection()
  table = con.table('inv_index')
  bat = table.batch()
  col_name = "docs:%s" %url_hash
  for term in doc_index:
    content = ""
    for pos in doc_index[term]:
      content += "%d " %(pos)

    bat.put(bytes(term, "utf-8"),
      {
      bytes(col_name, "utf-8") : bytes(content, "utf-8")
      })
    bat.counter_inc(bytes(term, "utf-8"), b'docs:counter')

  try:
    bat.send()
  except Excpetion as e:
    # It's possible that the connection timed out
    log.debug("Doc with url: %s suffered: %s" %(url_hash, e))


def get_occuring_docs(term):
  """
  Returns a dict of docs
  """
  con = get_hbase_connection()
  table = con.table('inv_index')
  row = table.row(bytes(term, "utf-8"))

  docs_dict = {}
  for doc_with_cf in row:
    docs_dict[doc_with_cf.strip("docs:")] = row[doc_with_cf]

  return docs_dict


create_crawl_count_table()