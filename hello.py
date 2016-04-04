from flask import Flask, request	
from flask import render_template
import res
from collections import deque
from math import ceil

app = Flask(__name__)

@app.route('/')
def hello_world():
	
    return render_template('indexf.html')

@app.route('/search/', methods=['POST', 'GET'])
def view_results(a = None):
	# if request.method == 'GET':
	# query the engine and get the results.
	search_query = request.args.get('q', '')
	and_q = request.args.get('cand', '')
	ph_q = request.args.get('cph', '')
	not_q = request.args.get('cnot', '')
	md = []

	type_id = request.args.get('tid','all')
	#if (not type_id):
	#	type_id = 'all'


	send_query = search_query + '&' + and_q + '&' + ph_q + '&' + not_q + '&' + type_id

	flag = 0
	cache_entry = ()
	# check in cache
	for entry in res_cache:
		if (send_query == entry[0]):
			cache_entry = entry
			flag = 1
			break

	# if hit then set else query the engine
	if (flag == 0):
		size, search_results, dym, time, file_res = gen_search.send(send_query)
		res_cache.append((send_query, size, search_results, dym, time, file_res))
	else:
		size, search_results, dym, time, file_res = cache_entry[1], cache_entry[2], cache_entry[3], cache_entry[4], cache_entry[5] 

	
	# convert the results into pages
	pg_size = request.args.get('nr','')
	if (not pg_size):
		pg_size = 7
	pg_size = int(pg_size)

	pg = request.args.get('p','')
	if (not pg):
		pg = 1
	pg = int(pg)

	pgs = ceil(size / pg_size)

	start_nav = max(1, pg - 3)
	nav = (start_nav, min(pgs, start_nav + 7))

	start = pg_size * (pg - 1) + 1
	stop = start + pg_size
	pg_res = [search_results[i] for i in range(start - 1, min(stop, size))]

	# make a list of results and a dict object of metadata
	entries = [dict(res_title = resl[0], link = resl[1], rank = resl[2], desc = resl[3]) for resl in pg_res]
	md = dict(num_res = size, next_pg = pg + 1, prev_pg = pg - 1, pgs = pgs, \
			search_query = search_query, and_q = and_q, ph_q = ph_q, not_q = not_q, \
			 start = start, stop = stop - 1, dym = dym, \
			 time = time, nav = nav, pg_size = pg_size, type_id = type_id)	

	#render
	return render_template('results2.html', entries = entries, md = md, file_res = file_res)


if __name__ == '__main__':

	gen_search = res.query_func(None)
	gen_search.send(None)

	# result cache
	res_cache = deque(maxlen = 10)

	app.debug = True
	app.run(host='0.0.0.0')