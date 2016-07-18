
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer
from nltk import WordNetLemmatizer
import string


PUNCTUATION = set(string.punctuation)
STOPWORDS = set(stopwords.words('english'))
STEMMER = PorterStemmer()
LEMMATIZER = WordNetLemmatizer()

def readStopWords(filename):
	s = []
	with open(filename,"r") as sfile:
		for line in sfile:
			#line.rstrip()
			#line.lstrip()
			s.append(line)
	return s

#def processing(text,stopW,punS):
def processing(text):
	raw_token = word_tokenize(text)
	l_raw_token = [t.lower() for t in raw_token]
	
	#print stopW.value[0]
	wpunct = []
	for tok in l_raw_token:
		word = ''.join([c for c in tok if not c in PUNCTUATION])
		wpunct.append(word)

	#print stopW.value[0]

	wstop = [word for word in wpunct if not word in STOPWORDS]

	#stemmedText = [STEMMER.stem(w) for w in wstop]
	stemmedText = [LEMMATIZER.lemmatize(t) for t in wstop]
	sending = [w for w in stemmedText if w]
	return sending