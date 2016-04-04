import pickle
import datetime
from difflib import * 
from search import *
from adv_srch import *
import re

def timestamp():
    return int((datetime.datetime.utcnow() - datetime.datetime(1970, 1, 1)).total_seconds() * 1000)

def query_func(send_query = None):

     

    def getItem(item):
     return item[1]

    search_results = []
    rank = []
    did_u = []
    time = None
    send_query = ''
    file_res = ()


    with open('company_data.pkl', 'rb') as input:
        inv_index = pickle.load(input)
        url_mapper = pickle.load(input)
        file_to_terms = pickle.load(input)
        url_inv_index = pickle.load(input)
        doc_len = pickle.load(input)
        titl = pickle.load(input)
        idf = pickle.load(input)
        uidf = pickle.load(input)
        url_len = pickle.load(input)
        url_to_terms = pickle.load(input)

    with open('company_data2.pkl', 'rb') as input:    
        pr=pickle.load(input)


    all_words=set(inv_index.keys())
    all_words=all_words | set(url_inv_index.keys())

    # print("UNPICKLED")    
    while True:

        send_query = yield len(rank), search_results, did_u, time, file_res

        send_query = send_query.split('&')
        search_query = send_query[0]
        and_q = send_query[1]
        ph_q = send_query[2]
        not_q = send_query[3]
        type_id = send_query[4]
        if (type_id == 'all'):
            type_id = ''

        stime = timestamp()
        rank, search_results, did_u = [], [], ''

        rank,file_word,string=advanced_search(search_query, and_q, ph_q, not_q, type_id, inv_index, \
                                             url_inv_index, idf, uidf, doc_len, url_len, pr, url_to_terms)

        pattern=re.compile('[\W_]+')
        strin=pattern.sub(' ',search_query+' '+and_q+' '+ph_q)
        did_u=''
        fl=False
        for word in strin.lower().split():
            if word in  all_words:
                did_u+=word+' '
            else:
                temp=get_close_matches(word,all_words,1)
                if temp==[]:
                   did_u+=word+' '
                else:
                    did_u+=temp[0]+' '   
                    fl=True
        if (fl == False):
            did_u = None

        #if rank==[]:
        #    print("NO RESULTS FOUND")

        rank=sorted(rank,key=getItem,reverse=True)
        # print("No of Links",len(rank))
        ftime = timestamp()
        time = (ftime - stime)/1000
        n_html, n_pdf, n_doc, n_pic, n_other = 0, 0, 0, 0, 0

        for r in range(len(rank)):
            try:
            # title not always present for links -- titl[rank[r][0]]
                titl[rank[r][0]]
            except (NameError, KeyError):
                titl[rank[r][0]] = url_mapper[rank[r][0]]

            if (titl[rank[r][0]] == ''):
                titl[rank[r][0]] = url_mapper[rank[r][0]]
            
            if 'html' in url_to_terms[rank[r][0]]:
                n_html+=1
            elif( 'pdf' in url_to_terms[rank[r][0]]):
                n_pdf+=1
            elif ('doc' in url_to_terms[rank[r][0]]):
                n_doc+=1
            elif ('doc' in url_to_terms[rank[r][0]]):
                n_pic+=1
            else :
                n_other+=1
                
            temp=sorted(file_word[rank[r][0]])
            emp='...'  
            try:
                ctr=temp[0]
                for x in range(max(ctr-7,0),ctr):
                    emp+=file_to_terms[rank[r][0]][x]+' '
                emp+='<b>' +file_to_terms[rank[r][0]][ctr]+'</b> '
                for i in range(1,len(temp)):
                    if (temp[i]-ctr)<10:
                        for x in range(ctr+1, temp[i]):
                            emp+=file_to_terms[rank[r][0]][x]+' '
                        emp+='<b>'+ file_to_terms[rank[r][0]][temp[i]]+'</b> '
                    else:
                        for x in range(ctr+1,ctr+5):
                            emp+=file_to_terms[rank[r][0]][x]+' '
                        emp+='... '
                        for x in range(max(temp[i]-4,0),temp[i]):
                            emp+=file_to_terms[rank[r][0]][x]+' '
                        emp+='<b>'+ file_to_terms[rank[r][0]][temp[i]]+'</b> '
                    ctr=temp[i]
                for x in range(ctr+1,ctr+6):
                    emp+=file_to_terms[rank[r][0]][x]+' '
                emp+='... '
            except:
                pass
            print(url_mapper[rank[r][0]]    )

            search_results.append((titl[rank[r][0]], url_mapper[rank[r][0]], rank[r], emp))

        file_res = dict(html=n_html, pdf=n_pdf, doc=n_doc, pic=n_pic, other=n_other)



if __name__ == '__main__':

    while(True):
        search_query = input('Enter a string to search: ')
        query_func(search_query = search_query)

