import re


def one_word_query(word, invertedIndex):
	pattern = re.compile('[\W_]+')
	word = pattern.sub(' ',word)
	if word in invertedIndex.keys():
		return [filename for filename in invertedIndex[word].keys()]
	else:
		return []



def free_text_query(string,invertedIndex):
	pattern = re.compile('[\W_]+')
	string = pattern.sub(' ',string)
	
	result = []
	for word in string.lower().split():
		result += one_word_query(word,invertedIndex)
	return list(set(result)),string

def and_query(string,invertedIndex):
	pattern = re.compile('[\W_]+')
	string = pattern.sub(' ',string)
	i=0
	result = set([])
	for word in string.lower().split():
		if i==0:
		    result=set(one_word_query(word,invertedIndex))
		    i=i+1
		else:
		    temp_set= set(one_word_query(word,invertedIndex))
		    result=result & temp_set
	return list(result)

def phrase_query(string, invertedIndex): 
	pattern = re.compile('[\W_]+')
	string = pattern.sub(' ',string)
	
	listOfLists, result = [],[]
	for word in string.lower().split():
		listOfLists.append(one_word_query(word,invertedIndex))
	setted = set(listOfLists[0]).intersection(*listOfLists)
	for filename in setted:
		temp = []
		for word in string.lower().split():
			temp.append(invertedIndex[word][filename][:])
		for i in range(len(temp)):
			for ind in range(len(temp[i])):
				temp[i][ind] -= i
		if set(temp[0]).intersection(*temp):
			result.append(filename)
	return result


    


