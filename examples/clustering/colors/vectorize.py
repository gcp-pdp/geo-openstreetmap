import re
import textract
from pathlib import Path
import numpy as np
import json

import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer

nltk.download('punkt')

nltk.download('stopwords')
stop_words = set(stopwords.words('english')).union({'etc', 'note', 'also', 'occur'})

nltk.download('wordnet')
lemmatizer = WordNetLemmatizer()

embeddings_dict = {}

with open("../data/glove.6B.300d.txt", 'r', encoding="utf-8") as f:
    for line in f:
        values = line.split()
        word = values[0]
        vector = np.asarray(values[1:], "float32")
        embeddings_dict[word] = vector

rows = [
    {'dimension': 'Activity', 'code': 1000, 'color': 'FF00FF', 'name': 'Residential activities'},
    {'dimension': 'Activity', 'code': 2000, 'color': 'FF0000', 'name': 'Shopping, business, or trade activities'},
    {'dimension': 'Activity', 'code': 3000, 'color': 'A0F020',
     'name': 'Industrial, manufacturing, and waste- related activities'},
    {'dimension': 'Activity', 'code': 4000, 'color': '00FF00',
     'name': 'Social, institutional, or infrastructure- related activities'},
    {'dimension': 'Activity', 'code': 5000, 'color': 'BEBEBE', 'name': 'Travel or movement activities'},
    {'dimension': 'Activity', 'code': 6000, 'color': '2F4F4F', 'name': 'Mass assembly of people'},
    {'dimension': 'Activity', 'code': 7000, 'color': '9090EE', 'name': 'Leisure activities'},
    {'dimension': 'Activity', 'code': 8000, 'color': '22228B', 'name': 'Natural resources-related activities'},
    {'dimension': 'Activity', 'code': 9000, 'color': 'FFFFFF', 'name': 'No human activity or unclassifiable activity'},

    {'dimension': 'Function', 'code': 1000, 'color': 'FF00FF', 'name': 'Residence or accommodation functions'},
    {'dimension': 'Function', 'code': 2000, 'color': 'FF0000', 'name': 'General sales or services'},
    {'dimension': 'Function', 'code': 3000, 'color': 'A0F020', 'name': 'Manufacturing and wholesale trade'},
    {'dimension': 'Function', 'code': 4000, 'color': 'BEBEBE',
     'name': 'Transportation, communication, information, and utilities'},
    {'dimension': 'Function', 'code': 5000, 'color': '9090EE', 'name': 'Arts, entertainment, and recreation'},
    {'dimension': 'Function', 'code': 6000, 'color': '00FF00',
     'name': 'Education, public admin., health care, andother inst.'},
    {'dimension': 'Function', 'code': 7000, 'color': '008B8B', 'name': 'Construction-related businesses'},
    {'dimension': 'Function', 'code': 8000, 'color': '558B00', 'name': 'Mining and extraction establishments'},
    {'dimension': 'Function', 'code': 9000, 'color': '22228B', 'name': 'Agriculture, forestry, fishing and hunting'},

    {'dimension': 'Ownership', 'code': 1000, 'color': 'F5DCF5', 'name': 'No constraints--private ownership'},
    {'dimension': 'Ownership', 'code': 2000, 'color': '00FF00',
     'name': 'Some constraints--easements or other use restrictions'},
    {'dimension': 'Ownership', 'code': 3000, 'color': '008B00',
     'name': 'Limited restrictions--leased and other tenancy restrictions'},
    {'dimension': 'Ownership', 'code': 4000, 'color': '9090EE',
     'name': 'Public restrictions--local, state, and federal ownership'},
    {'dimension': 'Ownership', 'code': 5000, 'color': '000064',
     'name': 'Other public use restrictions--regional, special districts, etc'},
    {'dimension': 'Ownership', 'code': 6000, 'color': '6B238E', 'name': 'Nonprofit ownership restrictions'},
    {'dimension': 'Ownership', 'code': 7000, 'color': 'BEBEBE', 'name': 'Joint ownership character--public entities'},
    {'dimension': 'Ownership', 'code': 8000, 'color': '000000',
     'name': 'Joint ownership character--public, private, nonprofit, etc.'},
    {'dimension': 'Ownership', 'code': 9000, 'color': 'FFFFFF', 'name': 'Not applicable to this dimension'},

    {'dimension': 'Site', 'code': 1000, 'color': '9090EE', 'name': 'Site in natural state'},
    {'dimension': 'Site', 'code': 2000, 'color': 'F5DCF5', 'name': 'Developing site'},
    {'dimension': 'Site', 'code': 3000, 'color': 'CD9EB7', 'name': 'Developed site -- crops, grazing, forestry, etc.'},
    {'dimension': 'Site', 'code': 4000, 'color': '8B667E', 'name': 'Developed site -- no buildings and no structures'},
    {'dimension': 'Site', 'code': 5000, 'color': '8B2B00', 'name': 'Developed site -- nonbuilding structures'},
    {'dimension': 'Site', 'code': 6000, 'color': '8B2323', 'name': 'Developed site -- with buildings'},
    {'dimension': 'Site', 'code': 7000, 'color': '22228B', 'name': 'Developed site -- with parks'},
    {'dimension': 'Site', 'code': 8000, 'color': 'D3D3D3', 'name': 'Not applicable to this dimension'},
    {'dimension': 'Site', 'code': 9000, 'color': 'FFFFFF', 'name': 'Unclassifiable site development character'},

    {'dimension': 'Structure', 'code': 1000, 'color': 'FF00FF', 'name': 'Residential buildings'},
    {'dimension': 'Structure', 'code': 2000, 'color': 'FF0000', 'name': 'Commercial buildings and other specialized structures'},
    {'dimension': 'Structure', 'code': 3000, 'color': 'A0F020', 'name': 'Public assembly structures'},
    {'dimension': 'Structure', 'code': 4000, 'color': '00FF00', 'name': 'Institutional or community facilities'},
    {'dimension': 'Structure', 'code': 5000, 'color': 'BEBEBE', 'name': 'Transportation-related facilities'},
    {'dimension': 'Structure', 'code': 6000, 'color': '858585', 'name': 'Utility and other nonbuilding structures'},
    {'dimension': 'Structure', 'code': 7000, 'color': 'FFCBC0', 'name': 'Specialized military structures'},
    {'dimension': 'Structure', 'code': 8000, 'color': '22228B', 'name': 'Sheds, farm buildings, or agricultural facilities'},
    {'dimension': 'Structure', 'code': 9000, 'color': 'FFFFFF', 'name': 'No structure'}
]


def tokenize(text):
    tokens = nltk.word_tokenize(text.lower().replace('-', ' '))
    filtered = [t for t in tokens if t not in stop_words and t.isalpha()]

    return [lemmatizer.lemmatize(t) for t in filtered]


def mean_vector(tokens):
    count = 0
    sum_vector = np.zeros(300)
    for token in tokens:
        if token not in embeddings_dict:
            continue
        sum_vector += embeddings_dict[token]
        count += 1

    return sum_vector / count


def vectorize():
    try:
        text = Path('../data/LBCS.txt').read_text()
    except FileNotFoundError:
        text = textract.process('../data/LBCS.pdf')
        text = text.decode('utf-8')
        Path('../data/LBCS.txt').write_text(text)

    # split text to parts with each dimension details
    dimensions_details = re.split('\w Dimension with Detail', text)[1:]
    # remove text tail from the last part
    dimensions_details[-1] = re.split('LBCS Top Level Codes for all Dimensions', dimensions_details[-1])[0]

    vectors = []
    for dimension in dimensions_details:
        # split by classes codes
        codes = re.split('1000|2000|3000|4000|5000|6000|7000|8000|9000|9999', dimension)[1:10]
        for code in codes:
            tokenized = tokenize(code)
            vec = mean_vector(tokenized)
            vectors.append(vec)

    for (row, vec) in zip(rows, vectors):
        for i in range(len(vec)):
            row['f{}'.format(i+1)] = vec[i]
        print(json.dumps(row))


if __name__ == '__main__':
    vectorize()
