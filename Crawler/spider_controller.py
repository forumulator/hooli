import hashlib
import logging
import re
import sys
import time
import threading

from collections import deque

import hbase_util

from crawler import executeCrawler
from index_mgr_obj import index_mgr

POST_TO_DB_TIME_INTERVAL = 20
SetOfURLs_KEY = "setOfURLs"
HTMLString_KEY = "htmlString"
pattern = re.compile("[\W_]+")
Wiki_substring = 'en.wikipedia.org/wiki/'

class spider_controller:

  def __init__(self, threadsMaxNumber, pagesMaxNumber, start_url,
    resume_flag=False):

    self.__threadsMaxNumber = threadsMaxNumber
    self.__pagesMaxNumber = pagesMaxNumber
    self.__tot_crawl_count = 0
    # Stores URLs that still need to be crawled
    # pops from left, and appends from right
    self.__urlToCrawlDeque = deque()

    # stores the current threads - as numbers 1,2,..
    self.__threadDict = {}

    self.__numberOfURLsCrawled = 0
    self.__lastPostTimeToDB = time.time()

    # stores hashed of urls already crawled
    self.__alreadyCrawledSet = set()
    self.__alreadyCrawledContentSet = set()
    # stores key: thread_id and value: URL being crawled by the thread
    self.__beingCrawledDict = {}
    
    # Not private coz it is being passed to threads
    self.threadReturnDataDict = {}

    # Initialize the index manager
    self.__indx_mgr = index_mgr
    # Initial URL added to deque
    if not resume_flag:
      self.__urlToCrawlDeque.append(start_url)
    else:
      self.__resume_queue()

    ####debugging
    self.__startTime = time.time()

    print("spider_controller.__init__() complete")

  def run(self):

    while True:
      
      self.__postToDBIfValid()

      self.__getDataFromFinishedThreads()

      # Both __urlToCrawlDeque and __threadDict are empty,
      # so spider's job is done, hence exit
      if len(self.__urlToCrawlDeque) == 0 and len(self.__threadDict) == 0:
        print("I'm out")
        exit(0)

      # __urlToCrawlDeque is empty, but threads running
      if len(self.__urlToCrawlDeque) == 0:
        logging.info("__urlToCrawlDeque empty, going to sleep for 5 seconds")
        time.sleep(2)
        continue

      while (len(self.__urlToCrawlDeque) != 0 and
             len(self.__threadDict) < self.__threadsMaxNumber):
        url = self.__urlToCrawlDeque.popleft()
        url_hash = hashlib.sha256(bytes(url, "utf-8")).hexdigest()
        if url in self.__alreadyCrawledSet:
          continue
        # check if the url is already crawled, i.e, present in database.
        # Then cache in the already CrawledSet
        if hbase_util.check_web_doc(url_hash):
          self.__alreadyCrawledSet.add(url_hash)
          continue
        #else part
        # print("creating thread for %s"% url)
        for t_id in range(1,self.__threadsMaxNumber + 1):
          if self.__threadDict.get(t_id) is None:
            self.__createAndStartNewThread(t_id, url)
            break
        # time.sleep(1)

      # logging.info("Iteration Done. Sleeping for 1 seconds.")
      logging.info("%d pages crawled"%self.__tot_crawl_count)
      logging.info("%d seconds passed"%(time.time() - self.__startTime))
      time.sleep(1)

  def __postToDBIfValid(self):

    if time.time() - self.__lastPostTimeToDB > POST_TO_DB_TIME_INTERVAL:
      # post the count of the newly crawled urls and check
      # if the global limit has exceeded
      if (hbase_util.update_crawl_count(self.__numberOfURLsCrawled) >
        self.__pagesMaxNumber):
        logging.info("Pages limit reached. Stopping spider.")
        logging.info("The cache size is:\nurl: %s \ncontent: %s" %(
          sys.getsizeof(self.__alreadyCrawledSet),
          sys.getsizeof(self.__alreadyCrawledContentSet)))
        # storing the queue to database
        self.__store_queue()
        exit(0)
      self.__numberOfURLsCrawled = 0
      self.__lastPostTimeToDB = time.time()

  def __store_queue(self):
    url_str = ""
    while not self.__urlToCrawlDeque:
      url_str += self.__urlToCrawlDeque.popleft() + "\n"
    hbase_util.store_spider_queue(url_str)

  def __resume_queue(self):
    import socket
    self.__urlToCrawlDeque = deque(
      self.__indx_mgr.retrieve_spider_q(socket.gethostname()))


  def __getDataFromFinishedThreads(self):

    for threadID in list(self.__threadDict.keys()):

      if not self.__threadDict[threadID].is_alive():
        # print("thread %d is not alive"% threadID)

        # Check if any data has been added to the dict by the thread.
        if self.threadReturnDataDict.get(threadID) is not None:
          #----------------------------------
          # Strip CSS and JS from html_string
          #----------------------------------
          self.__processCrawledURL(threadID)
          self.__processSetOfURLsReturned(threadID)

          # delete the thread from the return dict and being crawled dict     
          del(self.threadReturnDataDict[threadID])
          del(self.__beingCrawledDict[threadID])

        # Otherwise add the URL back to the queue because
        # the thread has failed to process the URL
        else:
          self.__urlToCrawlDeque.append(self.__beingCrawledDict[threadID])
          del(self.__beingCrawledDict[threadID])


        # Delete thread from dict of threads
        del(self.__threadDict[threadID])

  def __get_html_text(self, html):
    """
    Returns the html text and title from the html page.
    """
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html)
    final_str = ""
    for tag in ["script","style"]:
        for s in soup.find_all(tag):
            s.replaceWith(" ")
    # get the title
    # ---------------
    # Check this code
    # ---------------
    html_title = ''
    for title in soup.find_all('title'):
        temp_soup = BeautifulSoup(str(title))
        title = temp_soup.get_text()
        html_title = title  
    try:
        ht = str(soup.prettify())
        # this is to prevent words from glueing together after
        # tags are removed
        soup2 = BeautifulSoup(ht)
        final_str = str(soup2.get_text())

    except:
        # the prettify() fn goes into infinte recursion
        # when <TABLE> is present        
        final_str = str(soup.get_text())

    return (pattern.sub(" ", final_str), html_title)

  def __processCrawledURL(self, threadID):

    self.__numberOfURLsCrawled += 1
    self.__tot_crawl_count += 1
    url = self.__beingCrawledDict[threadID]
    url_hash = hashlib.sha256(bytes(url, "utf-8")).hexdigest()
    self.__alreadyCrawledSet.add(url_hash)
    # Adding the web page details to the database.
    html_text, html_title = self.__get_html_text(
      self.threadReturnDataDict[threadID][HTMLString_KEY])
    #print(html_text)
    html_title_hash = hashlib.sha256(bytes(html_title, "utf-8")).hexdigest()
    if (not html_title_hash in self.__alreadyCrawledContentSet
     and not hbase_util.check_web_doc_content(html_title_hash)):
      hbase_util.post_web_doc(url_hash, url, html_text, html_title)
    else:
      logging.info("Given url: %s content is already present" %url)
    self.__alreadyCrawledContentSet.add(html_text_hash)

  def __processSetOfURLsReturned(self, threadID):

    # for each url, check if it is already crawled
    for url in self.threadReturnDataDict[threadID][SetOfURLs_KEY]:
      urlHash = hashlib.sha256(bytes(url, "utf-8")).hexdigest()

      if (Wiki_substring in url and urlHash not in self.__alreadyCrawledSet):
        # if(":" not in url[10:]):
        self.__urlToCrawlDeque.append(url)

  def __createAndStartNewThread(self,t_id, url):

    thread = threading.Thread(None, executeCrawler, 
             args=(url, self.threadReturnDataDict, t_id))

    self.__threadDict[t_id] = thread
    self.__beingCrawledDict[t_id] = url
    thread.start()











