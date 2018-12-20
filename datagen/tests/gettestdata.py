import os, sys, paramiko, pickle, re
from sklearn.feature_extraction.text import CountVectorizer
from dateutil.parser import parse
from scipy.sparse import csc_matrix

class getTestData:
	def __init__(self, config, datadir):
		self.localdir = config.get("amass", "datadir")
		self.resources = re.split("\s*,\s*", config.get("resources", "db"))

	def mapTests(self):
		run_python_script = "python map_tests_to_resources.py"
		os.system(run_python_script)

	def processTime(self, timestamp):
		#finds the difference in seconds of timestamp with a base timestamp b
		b = parse('2008-01-01T00:00:00.000-08:00')
		t = parse(timestamp)
		diff = t-b
		return (diff.days*24*3600+diff.seconds)

	def createDict(self, em):
		#create bag-of-words dictionary given a list of error messages
		if not em:return em
		vect = CountVectorizer(min_df = 2, stop_words='english')
		vect.fit_transform(em)
		dict_ = vect.get_feature_names()
		return vect

	def extractFeature(self, l, vect, b, d):
		#extract feature for a particular test
		if(len(b)>5):
			if(l[1]=="false"): return [self.processTime(l[0]), l[1]=="true", vect.transform([str(l[2]),])]
			else:return [self.processTime(l[0]), l[1]=="true", vect.transform([""])]
		elif(len(b)==1):return [self.processTime(l[0]), l[1]=="true", csc_matrix((0,0))]
		else:
			temp = csc_matrix((1,len(b)))
			if(l[1]=="false"): 
				temp[0,d[l[2]]]=1
				return [self.processTime(l[0]), l[1]=="true", temp]
			else:return [self.processTime(l[0]), l[1]=="true", temp]

	def preprocessTest(self,folder, tests):
		#extract features for all the tests in a folder
		files = os.listdir(folder)
		c = 0
		for each in files:
			error_messages = []
			if(each=='.DS_Store'):continue
			f = open(folder+'/'+each,'r')
			for line in f:
				l = line.strip().split("separator")
				if(l[1]=="false"):error_messages.append(l[2])
			b = list(set(error_messages))
			vect, d = None, {}
			c += 1
			print each, c
			if(len(b)>1):
				vect = self.createDict(error_messages)
				for i in range(len(b)):d[b[i]]=i
				if(len(b)>5):
					c+= len(vect.get_feature_names())
				else:
					c+= len(b)
			print each, c
			f = open(folder+'/'+each,'r')
			results = []
			for line in f:
				l = line.strip().split("separator")
				results.append(self.extractFeature(l,vect,b,d))
			results.sort()
			if(len(results)>0):tests[folder+'/'+each] = results
		return c
		
	def preprocessTests(self):
		#extract gordon and trestles tests one by one
		tests = {}
		for x in self.resources:
			a=self.preprocessTest(os.path.join(self.localdir,x), tests)
		pickle.dump(tests,open(os.path.join(self.localdir,'tests.pkl'),'wb'))

