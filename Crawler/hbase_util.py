# 
# Author: s.abhiram
#
# This file contains code for creating rows and querying the tables of Hbase.
#

import happybase
import logging
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
    if not b'crawl_statistics' in con.tables():
      con.create_table('crawl_statistics',
        {
          'stats': dict(),
        })
      logging.info("Crawl Stats Table created.")


    table = con.table('crawl_statistics')
    row = table.row(b'crawl')
    if not row:
      table.put(b'crawl', {b'stats:count': b'0'})
      logging.info("Row crawl created in the table.")

    if not b'web_doc' in con.tables():
      con.create_table('web_doc',
        {
          'details': dict(),
        })
      logging.info("Web docs Table created.")

  except Exception as e:
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
  row = table.row(b'crawl', columns=[b'stats:count'])
  db_count = row[b'stats:count'].decode("utf-8")
  db_count = str(int(db_count) + count)
  table.put(b'crawl', {b'stats:count': bytes(db_count, "utf-8")})
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

  logging.info("Stored the web page in the database in %s sec." %(time.time() - t1))

def check_web_doc(urlhash):
  '''
  Checks if the given url hash is already present in the database.
  Args: urlhash - <str> denoting the id of row to search.
  Returns True if there exists a row with the given id, False otherwise.
  '''
  t1 = time.time()
  con = get_hbase_connection()
  table = con.table('web_doc')
  row = table.row(bytes(urlhash, "utf-8"))
  logging.info("Checked the database in %s sec."%(time.time() - t1))
  if not row:
    return False
  logging.info("Given urlhash is already present in the database.")
  return True

create_crawl_count_table()