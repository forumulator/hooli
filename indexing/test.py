from pageRank import pageRank
import pickle
links = [[]]
num=55
def read_file(filename):
    f = open(filename, 'r')
    for line in f:
       
        (frm, to) = map(int, line.split(" "))
        extend = max(frm - len(links), to - len(links)) + 1
        for i in range(extend):
            links.append([])
        links[frm].append(to)
    f.close()


read_file('graph.txt')
pr =  pageRank(links, alpha=0.85, convergence=0.00001, checkSteps=10)
#Computing pageRank

for i in range(len(pr)):
	pr[i]=1.34*pr[i]/(pr[i]+(1.36/74000))
# Computing pageRank in the form required for integrating with base bmf ranking
# S=w*S/(S+k)
  
with open('company_data2.pkl', 'wb') as output:
    pickle.dump(pr, output, pickle.HIGHEST_PROTOCOL)
