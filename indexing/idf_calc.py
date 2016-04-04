from math import *

def idf(inv_index,number_of_files,url_inv_index,number_of_links):
    idf={}
    for word in inv_index.keys():
    	temp=len(inv_index[word].keys())
    	idf[word]=log10(number_of_files/temp)
    uidf={}
    for word in url_inv_index.keys():
    	temp=len(url_inv_index[word].keys())
    	uidf[word]=log10(number_of_links/temp)

    return idf,uidf
