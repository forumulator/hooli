
def update(doc_len,file_to_terms,number_of_files):
	sum=0
	for fil in file_to_terms.keys():
		sum+=doc_len[fil]
	sum=float(sum)/number_of_files
	for fil in file_to_terms.keys():
		doc_len[fil]/=sum
	return doc_len