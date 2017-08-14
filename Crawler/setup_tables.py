import happybase
import logging
import sys
import time
import traceback

import logger
from config import *

def get_hbase_connection():
  return happybase.Connection(host=thrift_host, port=thrift_port)

def setup_tables():
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
    table.counter_set(b'index', b'stats:avg_dl', 0)
    logging.info("Index Stats counter created")

  except Exception as e:
    # Print stack trace
    tb = traceback.format_exc()
    logging.error("Problem %s:\n%s" %(e, tb))
    return False

  return True

if __name__ == "__main__":
  logger.initialize()
  setup_tables()