import Pyro4
import threading
# import happybase
# import hbase_util as HBase
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
		# Crawling done flag
		self.f_crawling_done = False
		conn = HBase.get_hbase_connection()
		# Get the total number of docs crawled from the
		# crawl stats table
		crawl_stats = conn.table('crawl_statistics')
		self.doc_count = int(crawl_stats.counter_get(b'crawl', b'stats:count'))

		# Get the processed docs from the stats table
		stats_table = conn.table('index_stats')
		self.processed = int(stats_table.counter_get(b'index', 
			b'stats:counter'))

		self.tot_doc_len = HBase.get_tot_doc_len()
		
		logging.info("Initialising index manager with \
			doc_count: %d, processed_count: %d, tot_doc_len: %d" \
			%(self.doc_count, self.processed, self.tot_doc_len))

		print("Initialising index manager with \
			doc_count: %d, processed_count: %d, tot_doc_len: %d" \
			%(self.doc_count, self.processed, self.tot_doc_len))

		# necessary because multiple RPC run on different threads
		self.lock = threading.Lock()

	@property
	def p_doc_count(self):
		return self.doc_count

	@property
	def p_processed(self):
		return self.processed

	@property
	def p_tot_doc_len(self):
		return self.tot_doc_len

	@property
	def p_avg_doc_len(self):
		tot, num = self.tot_doc_len, self.doc_count
		return (tot / num)

	def crawling_done(self):
		self.f_crawling_done = True

	def is_crawling_done(self):
		return self.f_crawling_done

	def retrieve_docs_ids(self, num):
		"""
		Return the (starting id, number of docs) with the maximum
		number == num. If num docs are unavailable, returns the
		maximum number possible
		"""
		# print("Index_mgr: Retrieving %d doc ids" % num)
		self.lock.acquire()
		unindexed = self.doc_count - self.processed
		giving, start = 0, self.processed + 1
		
		if unindexed >= num:
			giving = num
		else:
			giving = unindexed
		
		if (giving > 0):
			self.processed += giving
		else:
			giving = 0
		self.lock.release()
		return (giving, start)

	def add_crawl_count(self, num):
		self.lock.acquire()
		self.doc_count += num
		count = self.doc_count
		self.lock.release()
		
		return count

	def update_tot_doc_len(self, leng):
		self.lock.acquire()
		self.tot_doc_len += leng
		ret = self.tot_doc_len
		self.lock.release()
		
		return ret

	def retrieve_spider_q(self, host_name):
		self.lock.acquire()
		url_str = hbase_util.retrieve_spider_queue(host_name)
		self.lock.release()
		return url_str
		


def main():
	import socket
	s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	s.connect(('172.16.114.80', 1))  # connect() for UDP doesn't send packets
	host_ip = s.getsockname()[0]
	daemon = Pyro4.Daemon(host=host_ip)
	uri = daemon.register(IndexManager)
	hbase_util.publish_manager_uri(uri)
	daemon.requestLoop()

if __name__ == "__main__":
	main()