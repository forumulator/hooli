import hbase_util as mine

s = mine.retrieve_docs_content(1)
y=0
for k in s:
	y=s[k]
	print(s[k])


# print([:100])