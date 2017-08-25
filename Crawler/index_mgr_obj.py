import Pyro4

import hbase_util

from config import *

def connect_to_index_mgr(uri = None):
  if uri is None:
    uri = hbase_util.get_manager_uri()
  index_mgr = Pyro4.Proxy(uri)
  if index_mgr is None:
    logging.error("Couldn't connect to IndexManager with URI: %s" %uri)
    raise RuntimeError("Couldn't connect to IndexManager with URI: %s" %uri)

  return index_mgr

# Global index_mgr object, to update
# the crawled doc count
index_mgr = connect_to_index_mgr(index_mgr_uri)
