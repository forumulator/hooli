import pickle

print("Unpickled")

with open('company_data.pkl', 'rb') as input:
    inv_index = pickle.load(input)
    url_mapper = pickle.load(input)
    index = pickle.load(input)
    file_to_terms = pickle.load(input)
    url_inv_index = pickle.load(input)
    doc_len = pickle.load(input)
    titl = pickle.load(input)


print("Unpickled")

