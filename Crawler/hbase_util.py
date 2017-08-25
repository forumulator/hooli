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
from config import *

thrift_port = 9001
thrift_host = "172.16.114.80"
depth = 3

def utf(string):
  return bytes(string, "utf-8")

def get_hbase_connection():
  return happybase.Connection(host=thrift_host ,port=thrift_port)

def publish_manager_uri(uri):
  """
  Publishes the give index manager uri to the Hbase table
  index_stats
  """
  con = get_hbase_connection()
  table = con.table("index_stats")
  table.put(b'index',
    {
      b'stats:uri': bytes(uri, "utf-8")
    })

def get_manager_uri():
  """
  Returns the index manager URI
  """
  con = get_hbase_connection()
  table = con.table("index_stats")
  row = table.row(b'index')
  return row[b'stats:uri'].decode("utf-8")

def update_crawl_count(count):
  '''
  Updates the Hbase crawl count by count and returns the new value.
  '''
  # Update in Hbase crawl_stats table
  t1 = time.time()
  con = get_hbase_connection()
  table = con.table('crawl_statistics')
  new_count = table.counter_inc(b'crawl', b'stats:count', value=count)
  logging.info("Updated hbase crawl count to %s in %s sec" %(new_count,
      time.time() - t1))

  return int(new_count)

def store_spider_queue(url_str):
  import socket
  host_name = socket.gethostname()
  con = get_hbase_connection()
  table = con.table("crawl_statistics")
  table.put(bytes(host_name, "utf-8"),
    {
      b'stats:url_queue': bytes(url_str, "utf-8")
    })
  logging.info(host_name+" successfully stored the crawling queue in Hbase")

def retrieve_spider_queue(host_name):
  con = get_hbase_connection()
  table = con.table("crawl_statistics")
  row = table.row(bytes(host_name, "utf-8"))
  old_host = host_name
  if not row:
    # Do I need to put a limit
    for key, r in table.scan():
      if key not b'crawl':
        row = r
        old_host = key.decode("utf-8")
        break
  if not row:
    return None
  table.delete(bytes(old_host, "utf-8"))
  logging.info("%s was given the queue of %s" %(host_name, old_host))
  return row["stats:url_queue"].decode("utf-8")


def post_web_doc(url_hash, url, html_string, html_title):
  '''
  Created a new record in the database for the given entry.
  '''
  t1 = time.time()
  con = get_hbase_connection()
  table = con.table('web_doc')
  table.put(bytes(url_hash, "utf-8"), 
    {
      b'details:url'  : bytes(url, "utf-8"),
      b'details:html' : bytes(html_string, "utf-8"),
      b'details:title': bytes(html_title, "utf-8"),
    })
  # Stores the content of the web doc
  content_hash = hashlib.sha256(bytes(html_title, "utf-8")).hexdigest()
  table = con.table('web_doc_content')
  table.put(bytes(content_hash, "utf-8"),
    {
      b'details:present' : bytes("1", "utf-8")
    })

  con = get_hbase_connection()
  table = con.table('enumerate_docs')
  num_id = table.counter_inc(b'gen_next_id', b'cf1:counter')
  table.put(bytes(str(num_id), "utf-8"),
    {
      b'cf1:urlhash': bytes(url_hash, "utf-8")
    })
  logging.info("Stored the web page in the database in %s sec." %(
    time.time() - t1))

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


def update_inv_index(url_hash, doc_index, max_tf):
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
      b'details:term_count' : bytes(str(term_count), "utf-8"),
      b'details:max_tf'     : bytes(str(max_tf), "utf-8"),
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
    doc_index[term] += "\n%s\n%s" %(term_count, max_tf)
    bat.put(bytes(term, "utf-8"),
      {
      bytes(col_name, "utf-8") : bytes(doc_index[term], "utf-8")
      })

  try:
    bat.send()
  except Exception as e:
    # It's possible that the connection timed out
    logging.error("Doc with url: %s suffered: %s" %(url_hash, e))

  logging.debug("Updated inv_index for doc: %s in %f time" %(url_hash,
    (time.time() - t1)))


def update_inv_index_batch(inv_index, metadata):
  t1 = time.time()
  term_count = len(doc_index)
  con = get_hbase_connection()

  table = con.table('web_doc')
  try:
    with table.batch(transaction=True) as batch:
      for doc, md in metadata:
        # |D| is needed for bm25 calculation
        batch.put(bytes(doc, "utf-8"),
          {
            b'details:term_count' : bytes(str(md[0]), "utf-8"),
            b'details:max_tf'     : bytes(str(md[1]), "utf-8"),
          })

  except Exception as e:
    logging.error("Batch write metadata for docs : %s failed" % ', '.join(list(metadata.keys())))


  # storing total term count to calculate average doc_length
  table = con.table("index_stats")
  table.counter_inc(b'index', b'stats:total_length', value=term_count)

  # Write the inverted index
  con = get_hbase_connection()
  table = con.table('inv_index')
  try:
    with table.batch(transaction=True) as bat:
      # TODO - try changing to with
      for term in inv_index:
        for doc in inv_index[term]:
          mdoc = metadata[doc]
          inv_index[term][doc] += "\n%s\n%s" %(mdoc[0], mdoc[1])

        term_dict = {}
        for doc in inv_index[term]:
          term_dict[utf(doc)] = utf(inv_index[term][doc])

        bat.put(bytes(term, "utf-8"), term_dict)
      
  except Exception as e:
    logging.error("Batch write inverted_index for docs : %s failed" 
        % ', '.join(list(metadata.keys())))

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
    time.time() - t1))

  return doc_list

def retrieve_inv_index(no_alphabets):
  t1 = time.time()
  con = get_hbase_connection()
  table = con.table('index_stats')
  last_id = table.counter_inc(b'inv_index', b'stats:counter', value=no_alphabets)
  if last_id >= (26**depth):
    return {}, False
  start_alpha = ""
  end_alpha = ""
  alph_sz = 26
  start_id = last_id - no_alphabets
  for i in range(depth):
    start_alpha = chr(ord('a') + start_id%alph_sz) + start_alpha
    end_alpha = chr(ord('a') + last_id%alph_sz) + end_alpha
    start_id //= 26
    last_id //= 26
  con = get_hbase_connection()
  table = con.table('inv_index')
  logging.info("Retrieving from: %s to %s" %(start_alpha, end_alpha))
  return (table.scan(row_start=bytes(start_alpha, "utf-8"),
      row_stop=bytes(end_alpha, "utf-8")), True)

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
    # change from docs:doc_id to doc_id
    doc_str = doc_with_cf.decode("utf-8")[5:]
    docs_dict[doc_str] = {}
    key_list = ["pos", "doc_len", "max_tf"]
    row[doc_with_cf] = row[doc_with_cf].decode("utf-8").split('\n')
    for i, item in enumerate(row[doc_with_cf]):
      if i > 0:
        # for scores
        item = float(item)
      else:
        # convert positions from string to list of <int>
        item = list(map(int, item.split()))
      docs_dict[doc_str][key_list[i]] = item
  logging.info("Retrived %s docs for term: %s in %f secs" %(len(docs_dict),
    term, time.time()-t1))
  return docs_dict


def remove_table(table_list=[]):
  """
  Disables and deletes the tables given in the list from Hbase.
  """
  con = get_hbase_connection()
  if table_list == []:
    table_list = con.tables()
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
    url_list.append(url.decode("utf-8"))
  logging.info("Retrived url for given list in %f sec" %(time.time()-t1))
  return url_list

def get_title(doc_list):
  t1 = time.time()
  title_list = []
  for doc in doc_list:
    con = get_hbase_connection()
    table = con.table('web_doc')
    title = table.row(bytes(doc, "utf-8"))[b'details:title']
    title_list.append(title.decode("utf-8"))
  logging.info("Retrived title for given list in %f sec" %(time.time()-t1))
  return title_list

def check_web_doc_content(content_hash):
  """
  Checks if the given content_hash is present in the Hbase Table.
  True if present and False otherwise.
  """
  con = get_hbase_connection()
  table = con.table('web_doc_content')
  row = table.row(bytes(content_hash, "utf-8"))
  if not row:
    return False
  return True

def get_avg_doc_len():
  t1 = time.time()
  conn = get_hbase_connection()
  table = conn.table('index_stats')
  avg_doc_len = table.counter_get(b'index', b'stats:avg_dl')
  logging.info("Retrieved avg doc len in %s secs" \
     %(time.time() - t1))

  return avg_doc_len


def get_tot_doc_len():
  t1 = time.time()
  conn = get_hbase_connection()
  table = conn.table('index_stats')
  tot_doc_len = table.counter_get(b'index', b'stats:total_length')
  logging.info("Retrieved tot doc len in %s secs" \
     %(time.time() - t1))

  return tot_doc_len

def get_max_tf(doc):
  con = get_hbase_connection()
  table = con.table('web_doc')
  max_tf = table.row(bytes(doc, "utf-8"))[b'details:max_tf']
  return int(max_tf.decode("utf-8"))

def get_doc_length(doc):
  con = get_hbase_connection()
  table = con.table('web_doc')
  term_count = table.row(bytes(doc, "utf-8"))[b'details:term_count']
  return int(term_count.decode("utf-8"))

def update_inv_index_score(term, value):
  con = get_hbase_connection()
  table = con.table('inv_index')
  try:
    table.put(term, value)
  except Exception as e:
    logging.error("While updating the inverted index for term: %s"
      "\ncaught the following error: %s" %(term, e))

def get_corpus_term_sz():
  con = get_hbase_connection()
  table = con.table('index_stats')
  return table.counter_get(b'index', b'stats:total_length')

if __name__ == "__main__":
  logger.initialize()
