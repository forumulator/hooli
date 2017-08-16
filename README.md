# Hooli
A distributed search engine

### Clone
```bash
git clone https://github.com/forumulator/hooli
```
### Dependencies
1. Python3
2. BeautifulSoup4
3. HappyBase
4. A Running Hbase Thrift server on port 9001
5. Pyro4
6. Flask

### Run the crawler

```bash
$ cd hooli/Crawler
$ python4 setup_tables.py
$ python3 indexmgr.py
```
Now copy the URI that is echoed onto the screen
```bash
$ python3 testCrawler.py
```
Enter the number of Pages and the URI above

### Run the indexer
```bash
$ cd hooli/Crawler
$ python3 indexer.py
```

### Run the search server
```bash
$ cd hooli
$ python3 hello.py
```

That's it, the server is running. Now you can go to the browser and search on localhost:5000.

### Brief code overview
`src/spider_controller`: The controller for the spiders
`src/crawler.py`: The actual crawler
`src/indexer.py`: The indexer class
`src/hbase_util.py`: Functions to implement the HBase Schema
`src/indexmgr.py`: The index Manager, handles the distributed indexing
`src/querying.py`: Handler the srach and ranking
`hello.py`: Flask code for the server
