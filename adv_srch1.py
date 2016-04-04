import re
from query import *
def advanced_search(string1,string2,string3,string4,typ,inv_index,url_inv_index,idf,uidf,doc_len,url_len,pr,url_to_terms):

    ind_doc,doc,stringa1=free_text_query(string1,inv_index)
    uind_doc,udoc,stringa1=free_text_query(string1,url_inv_index)
    all_doc1=doc|udoc
    flag=bool(all_doc1)
# checking for documnets containing OR Words
    if flag==False:
    	print("There are no files with atleast one of the OR words")

    doc=and_query(string2,inv_index)
    udoc=and_query(string2,url_inv_index)
    all_doc2=doc|udoc
    flag2=bool(all_doc2)
# checking for documnets containing AND Words    
    if flag2==False:
    	print("There are no files with atleast one of the AND words")

    doc=phrase_query(string3,inv_index)
    if doc==[]
    	print("No files with given phrase")	
# checking for documnets containing phrase words


    ind_doc,doc,stringa1=free_text_query(string4,inv_index)
    uind_doc,udoc,stringa1=free_text_query(string4,url_inv_index)
    all_doc3=doc|udoc
# checking for docs containing NOT words

    string=string1+' '+string2+' '+string3

    ind_doc,doc,string=free_text_query(string,inv_index)
    uind_doc,udoc,string=free_text_query(string,url_inv_index)

    all_doc=ind_doc|uind_doc
    all_doc=all_doc& all_doc1
    all_doc=all_doc& all_doc2
    all_doc=all_doc& set(doc)
    all_doc=all_doc- all_doc3
# ensuring that all the condtions are met
    if(bool(all_doc)):
    	print("NOT files expand all over")

    ind_doc=all_doc & ind_doc
    uind_doc=all_doc & uind_doc
    score,x,y={},{},{}
    rank=[]
    file_word={}

    for fil in all_doc:
        y[fil],x[fil],score[fil]=0,0,0
        file_word[fil]=[]

    for word in string.lower().split(): 
        
        for filename in ind_doc[word]:
                temp=len(inv_index[word][filename])
                x[filename]+=temp*(2.5)*idf[word]/(temp+1.5*(0.25+0.75*doc_len[filename]))
                file_word[filename].append(inv_index[word][filename][0])
        for fil in uind_doc[word]:
            temp=len(url_inv_index[word][fil])
            y[fil]+=temp*uidf[word]/url_len[fil]
            chec[fil]+=temp

    for fil in all_doc:
        score[fil]+=x[fil] + 0.1*y[fil]
        score[fil]+=pr[int(fil)]
        if(typ in url_to_terms[fil]):
	        rank.append((fil,score[fil],pr[int(fil)]))
    
    return rank,file_word,string

    

