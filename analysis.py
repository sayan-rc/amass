import pickle	
import random
import os, re, sys
import matplotlib
matplotlib.use('Agg')
import numpy as np
from sklearn import cross_validation
from sklearn import svm
from sklearn.linear_model import LogisticRegression
from sklearn.neighbors import NearestNeighbors, KNeighborsClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import matplotlib.pyplot as plt
from sklearn.metrics import classification_report
from sklearn.metrics import roc_curve, auc
from sklearn.metrics import f1_score
import time


def metrics(y_test,y_predict):
	tp, tn, fp, fn = 0, 0, 0, 0
	for i in range(len(y_test)):
		if(y_predict[i]==1):
			if(y_test[i]==1):
				tp = tp + 1
			else:
				fp = fp + 1
		else:
			if(y_test[i]==1):
				fn = fn + 1	
			else:
				tn = tn + 1
	s = float(sum((tp, tn, fp, fn)))
	return (tp, tn, fp, fn, tp/s, tn/s, fp/s, fn/s)		

def print_metrics(clf, X_train, X_test, y_train, y_test, name,filename):
	train_predict = clf.predict(X_train)
	test_predict = clf.predict(X_test)
	print("tp", "tn", "fp", "fn")
	print("Train metrics", metrics(y_train,train_predict))
	print("Test metrics", metrics(y_test,test_predict))
	(tp, tn, fp, fn, tps, tns, fps, fns) = metrics(y_test,test_predict)
	#print tp, tn, fp, fn, tps, tns, fps, fns
	print("Accuracy", float(tp+tn)/float(tp+fp+tn+fn))
	print("Precision(1)", float(tp)/float(tp+fp))
	if(not tn):
		print("Precision(0)", 0.0)
	else:	
		print("Precision(0)", float(tn)/float(tn+fn))
	print("Recall(0)", float(tn)/float(fp+tn))
	fpr, tpr, thresholds = roc_curve(y_test, clf.predict_proba(X_test)[:,1])
	plt.plot(fpr,tpr,'ro')
	plt.plot(fpr,tpr)
	plt.show()
	try:
		fpr, tpr, thresholds = roc_curve(y_test, clf.predict_proba(X_test)[:,1])
		plt.plot(fpr,tpr,'ro')
		plt.plot(fpr,tpr)
		plt.show()
	except:
		pred = clf.decision_function(X_test)
		fpr, tpr, thresholds = roc_curve(y_test, pred)
	roc_auc = auc(fpr,tpr)
	plt.clf()
	plt.plot(fpr, tpr, label='ROC curve (area = %0.2f)' % roc_auc)
	plt.xlabel('False Positive Rate')
	plt.ylabel('True Positive Rate')
	plt.title(name)
	plt.legend(loc="lower right")
	print("ROC curve (area = %0.2f)", roc_auc)
        print "Printing plot to plot.eps"
	plt.savefig(filename+'.eps', format='eps', dpi=1000)

def print_metrics2(clf, X_train, X_test, y_train, y_test):
	tpr = []
	fpr = []
	for t in np.arange(0,1,0.01):
		train_predict = list((np.array(clf.predict_proba(X_train)[:,0])>t)+0)
		test_predict = list((np.array(clf.predict_proba(X_test)[:,0])>t)+0)
		#print "tp", "tn", "fp", "fn"
		(tp, tn, fp, fn, tps, tns, fps, fns) = metrics(y_train,train_predict)
		(tp, tn, fp, fn, tps, tns, fps, fns) = metrics(y_test,test_predict)
		print (tp, tn, fp, fn)
		tpr.append(float(tp)/(tp+fn))
		fpr.append(float(fp)/(fp+tn))
	plt.plot(fpr,tpr)
	plt.show()
	
def getFiles():
	folds = os.listdir('data/')
	featfolders, labelfolders = [], []
	for each in folds:
		if each.endswith('X'):featfolders.append(each)
		if each.endswith('Y'):labelfolders.append(each)
	featfolders.sort()
        labelfolders.sort()
        f, l = [], []
        for i in range(len(labelfolders)):
                y = pickle.load(open('data/'+labelfolders[i]+'/1.pkl'))
                if len(y)>10:
                        f.append(featfolders[i])
                        l.append(labelfolders[i])
        return f, l

def splitData(featfold,labelfold, lastf):
	ffold = os.listdir('data/'+featfold+'/')
	featfiles=[]
	for each in ffold:
		featfiles.append(each)
	featfiles.sort()
	x=[]
	for file in featfiles:x.extend(pickle.load(open('data/'+featfold+'/'+file)))
	lfold = os.listdir('data/'+labelfold+'/')
        labelfiles=[]
        for each in lfold:
                labelfiles.append(each)
        labelfiles.sort()
        y=[]
        for file in labelfiles:y.extend(pickle.load(open('data/'+labelfold+'/'+file)))
	xtrain,xtest,ytrain,ytest=cross_validation.train_test_split(x,y,test_size=0.4,random_state=0)
	if lastf=='n':
		train, test = [], []
		for each in xtrain: train.append(each[:-1])
		for each in xtest: test.append(each[:-1])
		xtrain, xtest = train, test
	return xtrain,xtest,ytrain,ytest	

def createTrainTestData(listerr, lastf):
	X_train, X_test, y_train, y_test = [], [], [], []
	featfolders, labelfolders = getFiles()
	for i in listerr:
		print "Loading features for ", featfolders[i]
		ffold, lfold = featfolders[i], labelfolders[i]
		xtrain,xtest,ytrain,ytest = splitData(ffold, lfold, lastf)
		X_train.extend(xtrain)
		X_test.extend(xtest)
		y_train.extend(ytrain)
		y_test.extend(ytest)
		print "Length of data is "
		print "X_train: " + str(len(X_train))
		print "X_test: " + str(len(X_test))
		print "y_train: " + str(len(y_train))
		print "y_test: " + str(len(y_test))
	z = zip(X_train,y_train)
	random.shuffle(z)
	X_train,y_train = zip(*z)
	#pickle.dump(X_train,open('X_train','wb'))
	#pickle.dump(X_test,open('X_test','wb'))
	#pickle.dump(y_train,open('y_train','wb'))
	#pickle.dump(y_test,open('y_test','wb'))
	return X_train, X_test, y_train, y_test
 
def train(X_train, y_train):
	clf = RandomForestClassifier(random_state=0, n_estimators=50).fit(X_train, np.ravel(y_train))
	return clf

def test(clf, X_train, X_test, y_train, y_test, filename):
	print_metrics(clf, X_train, X_test, y_train, y_test, "Random forest", filename)

def printTopFeatures(clf, n):
	feat = list(clf.feature_importances_)
	imp = []
	for i in range(len(feat)):imp.append([feat[i],i])
	imp.sort(reverse=True)
	f = open('data/feat_names.pkl','rb')
	feat_names = pickle.load(f)
	numf = min(n, len(feat))
	for i in range(numf):print imp[i][0], imp[i][1]

def main(plotfileid):
	featfolders, _ = getFiles()
	print "Error indices are: "
	for i in range(len(featfolders)-1): print i, featfolders[i]

	print "Enter error indices you want to use separated by comma: "
	s = raw_input()
	indices = map(int, s.split(','))

	indices.append(len(featfolders)-1)
	
	print "Include \'last result\' features? (y/n)"
	a = raw_input()

	X_train, X_test, y_train, y_test = createTrainTestData(indices, a)
	start= time.clock()
	print "Start time " + str(start)
	clf = train(X_train, y_train)
	end = time.clock()
	print "End time " + str(end)
	print "Training time took " + str(time.clock()-start)
	print "Training included " + str(len(X_train)) + " x " + str(len(y_train))
	start= time.clock()
	print "Start time " + str(start)
	test(clf, X_train, X_test, y_train, y_test,"roc-" + s + "-" + a + "-" + plotfileid)
	end = time.clock()
	print "End time " + str(end)
	print "Testing time took " + str(time.clock()-start)
	print "Testing included " + str(len(X_test)) + " x " + str(len(y_test))
	printTopFeatures(clf, 10)	

if __name__ == "__main__":
	main(sys.argv[1])

