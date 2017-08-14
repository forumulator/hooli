

thrift_port = 9001
thrift_host = "localhost"

# TODO: Set this to an approp value
index_mgr_uri = None

# Starting URL
DEFAULT_CRAWL_URL = 'https://en.wikipedia.org/wiki/Rick_and_Morty'
# Limit of the number of pages to crawl
DEFAULT_CRAWL_MAX = 1000

DEFAULT_MAX_THREADS = 5


# tables
TB_CRAWL_STATS = b'crawl_statistics'
TB_ENUM_DOCS = b'enumerate_docs'
TB_WEB_DOC = b'web_doc'

TB_INDEX_STATS = b'index_stats'
TB_INV_INDEX = b'inv_index'
