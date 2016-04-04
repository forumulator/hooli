import query
import pickle
from main import *
from tf_idf import *
from difflib import * 
while(True):
 a=input('Enter a string to search: ')

#with open('company_data.pkl', 'rb') as input:
#    inv_index = pickle.load(input)
#    url_mapper = pickle.load(input)
#    tf_in = pickle.load(input)
#    word_list = pickle.load(input)


 word_set=set(word_list)
 uword_set=set(urlw_list)
 all_words=word_set | uword_set 
 docs,string=query.free_text_query(a,inv_index)
 udocs,string=query.free_text_query(a,url_inv_index)
 qry_vector=[]
 uqry_vector=[]

 docs.extend(udocs)
 docs=set(docs)

 did_u_mean=''
 flag=0
 for word in string.lower().split():
  if word in all_words:
   did_u_mean=did_u_mean+word+" "
   if word in word_set:
    i=word_list.index(word)
    qry_vector.append(i)

   if word in uword_set:
    i=urlw_list.index(word)
    uqry_vector.append(i)

  else:
   flag=1
   did_u_mean=did_u_mean+(get_close_matches(word,all_words,)[0])+" "


 rank=[]
 for filename in docs:
  x,y=0,0
  for i in qry_vector:
   x+=tf_in[filename][i]

  for i in uqry_vector:
   y+=utf_in[filename][i]
  rank.append((filename,x+0.75*y))

 def getItem(item):
  return item[1]

 rank=sorted(rank,key=getItem,reverse=True)
 t=min(len(rank),25)
 if flag==1:
  print("Did u mean: "+did_u_mean)
 print("RESULTS")
 for r in range(t):
  print(url_mapper[rank[r][0]], rank[r])
 print("\n\n")