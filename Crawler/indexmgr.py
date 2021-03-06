import Pyro4
import threading
import happybase
import hbase_util as HBase
import logging
import sys
import time

# This is a an alternative to locking
# Pyro4.config.SERVERTYPE = "multiplex"

@Pyro4.expose
@Pyro4.behavior(instance_mode="single")
class IndexManager:
	""" There needs to be only a single instance of
	this class running on a separate server, which would
	be accessed via RPCs to add_crawl_count and retrieve_doc_ids
	Assuming that all the crawled docs also need to be indexed
	"""
	def __init__(self):
		conn = HBase.get_hbase_connection()
		# Get the total number of docs crawled from the
		# crawl stats table
		crawl_stats = conn.table('crawl_statistics')
		self.doc_count = int(crawl_stats.counter_get(b'crawl', b'stats:count'))

		# Get the processed docs from the stats table
		stats_table = conn.table('index_stats')
		self.processed = int(stats_table.counter_get(b'index', 
			b'stats:counter', value=num_docs))
		
		logging.info("Initialising index manager with doc_count: %d, processed_count: %d" \
			%(self.doc_count, self.processed_count))

		# necessary because multiple RPC run on different threads
		self.lock = threading.Lock()

	@property
	def doc_count(self):
		return self.doc_count

	@property
	def processed(self):
		return self.processed

	def retrieve_docs_ids(self, num):
		"""
		Return the (starting id, number of docs) with the maximum
		number == num. If num docs are unavailable, returns the
		maximum number possible
		"""
		self.lock.acquire()
		unindexed = self.doc_count - self.processed
		giving, start = 0, self.doc_count + 1
		if unindexed >= num:
			giving = num
		else:
			giving = unindexed
		self.doc_count += giving
		self.lock.release()
		return (giving, start)

	def add_crawl_count(self, num):
		self.lock.acquire()
		self.doc_count += num
		count = self.doc_count
		self.lock.release()
		return count

def main():
	Pyro4.Daemon.serveSimple(
        {
            IndexManager: "indexMgr"
        },
        ns = False)

if __name__ == "__main__":
	main()