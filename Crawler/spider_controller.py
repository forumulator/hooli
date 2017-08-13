import hashlib
import re
import time
import threading

from collections import deque

import hbase_util

from crawler import executeCrawler

POST_TO_DB_TIME_INTERVAL = 20
SetOfURLs_KEY = "setOfURLs"
HTMLString_KEY = "htmlString"
pattern = re.compile("[\W_]+")
Wiki_substring = 'en.wikipedia.org/wiki/'

class spider_controller:

  def __init__(self, threadsMaxNumber, pagesMaxNumber, start_url):

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

    # stores key: thread_id and value: URL being crawled by the thread
    self.__beingCrawledDict = {}
    
    # Not private coz it is being passed to threads
    self.threadReturnDataDict = {}

    # Initial URL added to deque
    self.__urlToCrawlDeque.append(start_url)


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
        exit(0)

      # __urlToCrawlDeque is empty, but threads running
      if len(self.__urlToCrawlDeque) == 0:
        print("__urlToCrawlDeque empty, going to sleep for 5 seconds")
        time.sleep(5)
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
        print("creating thread for %s"% url)
        for t_id in range(1,self.__threadsMaxNumber + 1):
          if self.__threadDict.get(t_id) is None:
            self.__createAndStartNewThread(t_id, url)
            break
        time.sleep(1)

      print("Iteration Done. Sleeping for 2 seconds.")
      print("%d pages crawled"%self.__tot_crawl_count)
      print("%d seconds passed"%(time.time() - self.__startTime))
      time.sleep(2)

  def __postToDBIfValid(self):

    if time.time() - self.__lastPostTimeToDB > POST_TO_DB_TIME_INTERVAL:
      # post the count of the newly crawled urls and check
      # if the global limit has exceeded
      if (hbase_util.update_crawl_count(self.__numberOfURLsCrawled) >
        self.__pagesMaxNumber):
        print("Pages limit reached. Stopping spider.")
        exit(0)
      self.__numberOfURLsCrawled = 0
      self.__lastPostTimeToDB = time.time()

  def __getDataFromFinishedThreads(self):

    for threadID in list(self.__threadDict.keys()):

      if not self.__threadDict[threadID].is_alive():
        print("thread %d is not alive"% threadID)

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
    Returns the html text from the html page.
    """
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html)
    final_str = ""
    for tag in ["script","style"]:
        for s in soup.find_all(tag):
            s.replaceWith(" ")
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

    return pattern.sub(" ", final_str)

  def __processCrawledURL(self, threadID):

    self.__numberOfURLsCrawled += 1
    self.__tot_crawl_count += 1
    url = self.__beingCrawledDict[threadID]
    url_hash = hashlib.sha256(bytes(url, "utf-8")).hexdigest()
    self.__alreadyCrawledSet.add(url_hash)
    # Adding the web page details to the database.
    html_text = self.__get_html_text(self.threadReturnDataDict[threadID][HTMLString_KEY])
    #print(html_text)
    hbase_util.post_web_doc(url_hash, url, html_text)


  def __processSetOfURLsReturned(self, threadID):

    # for each url, check if it is already crawled
    for url in self.threadReturnDataDict[threadID][SetOfURLs_KEY]:
      urlHash = hashlib.sha256(bytes(url, "utf-8")).hexdigest()

      if (Wiki_substring in url and urlHash not in self.__alreadyCrawledSet):
        self.__urlToCrawlDeque.append(url)

  def __createAndStartNewThread(self,t_id, url):

    thread = threading.Thread(None, executeCrawler, 
             args=(url, self.threadReturnDataDict, t_id))

    self.__threadDict[t_id] = thread
    self.__beingCrawledDict[t_id] = url
    thread.start()











