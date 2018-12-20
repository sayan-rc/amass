import pickle	
import random
import os, re, sys
import matplotlib
matplotlib.use('Agg')
import numpy as np
from sklearn import cross_validation
from sklearn import svm
from sklearn.linear_model import LogisticRegression
from sklearn.svm import LinearSVC
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
	d={}
	dt={}
	for y in y_test:
		if y in d:
			d[y]+=1
		else:
			d[y]=1
	for y in d.keys():
		print y, d[y]
	for y in test_predict:
		if y in dt:
			dt[y]+=1
		else:
			dt[y]=1
	xt=open('checking.txt','w')
	#for i in xrange(len(test_predict)):
                #print>> xt, X_test[i][159],X_test[i][160], test_predict[i]
	for y in dt.keys():
		print y,dt[y]
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
	#fpr, tpr, thresholds = roc_curve(y_test, clf.predict_proba(X_test)[:,1])
	#plt.plot(fpr,tpr,'ro')
	#plt.plot(fpr,tpr)
	#plt.show()
	try:
		fpr, tpr, thresholds = roc_curve(y_test, clf.predict_proba(X_test)[:,1])
		#fpr1,tpr1, thresholds1=roc_curve(y_test[:int(len(y_test)*0.5)], clf.predict_proba(X_test[:int(len(X_test)*0.5)])[:,1])
		#fpr2, tpr2, thresholds2=roc_curve(y_test[int(len(y_test)*0.5):], clf.predict_proba(X_test[int(len(X_test)*0.5):])[:,1])
		fpr_train, tpr_train, thresholds_train = roc_curve(y_train, clf.predict_proba(X_train)[:,1])
		plt.plot(fpr,tpr,'ro')
		plt.plot(fpr,tpr)
		plt.show()
	except:
		pred = clf.decision_function(X_test)
		#pred1 = clf.decision_function(X_test[:int(len(X_test)*0.5)])
		#fpr1,tpr1, thresholds1=roc_curve(y_test[:int(len(y_test)*0.5)], pred1)
		#pred2 = clf.decision_function(X_test[int(len(X_test)*0.5):])
		#fpr2, tpr2, thresholds2=roc_curve(y_test[int(len(y_test)*0.5):], pred2)
		pred_train = clf.decision_function(X_train)
		fpr, tpr, thresholds = roc_curve(y_test, pred)
		fpr_train, tpr_train, thresholds_train = roc_curve(y_train, pred_train)
	#roc_auc1 = auc(fpr1,tpr1)
        #roc_auc2 = auc(fpr2,tpr2)
        #print("Area under ROC curve1",roc_auc1)
        #print("Area under ROC curve2",roc_auc2)
	roc_auc = auc(fpr,tpr)
	roc_auc_train = auc(fpr_train,tpr_train)
	plt.clf()
	plt.plot(fpr, tpr, label='ROC curve (area = %0.2f)' % roc_auc, marker='.')
	plt.xlabel('False Positive Rate')
	plt.ylabel('True Positive Rate')
	plt.title(name)
	plt.legend(loc="lower right")
        print "Printing plot to %s.eps" % filename
	plt.savefig(filename+'.eps', format='eps', dpi=1000)
	print("Area under ROC curve",roc_auc)
	print("Area under ROC curve train",roc_auc_train)
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
        featfolders, labelfolders, datefolders = [], [],[]
        for each in folds:
                if each.endswith('X'):featfolders.append(each)
                if each.endswith('Y'):labelfolders.append(each)
		if each.endswith('D'):datefolders.append(each)
        featfolders.sort()
        labelfolders.sort()
	datefolders.sort()
        f, l, d = [], [], []
        for i in range(len(labelfolders)):
                y = pickle.load(open('data/'+labelfolders[i]+'/1.pkl'))
                if len(y)>10:
                        f.append(featfolders[i])
			l.append(labelfolders[i])
			d.append(datefolders[i])
        return f, l, d

def splitData(featfold,labelfold, datefold, lastf):
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
        dfold = os.listdir('data/'+datefold+'/')
        datefiles=[]
        for each in dfold:
                datefiles.append(each)
        datefiles.sort()
        d=[]
        for file in datefiles:d.extend(pickle.load(open('data/'+datefold+'/'+file)))
	'''
	#ensuring the data is sorted
	z=zip(d,x,y)
	z.sort()
	d,x,y=zip(*z)
	#print x[0]
	train_len=int(0.6*len(x))
	xtrain=x[:train_len]
	xtest=x[train_len:]
	ytrain=y[:train_len]
	ytest=y[train_len:]
	#xtrain,xtest,ytrain,ytest=cross_validation.train_test_split(x,y,test_size=0.4,random_state=0)
	test=[]
	for each in xtest: test.append(each[1:])
	xtest=test
	if lastf=='n':
		train, test = [], []
		for each in xtrain: train.append(each[:-1])
		for each in xtest: test.append(each[:-1])
		xtrain, xtest = train, test
	'''
	return d,x,y

def filter(Xtrain, Ytrain):
	xsedeids = pickle.load(open('data/xsedeids.pkl'))
	print "Before filtering data size ", len(Xtrain)
	X,Y = [],[]
	for i in xrange(len(Xtrain)):
		#if Xtrain[i][0] in xsedeids or not Xtrain[i][0]:
		X.append(Xtrain[i][1:])
		Y.append(Ytrain[i])
	print "After filtering data size ", len(X)
	return X,Y    

def createTrainTestData(listerr, lastf):
	X_train, X_test, y_train, y_test = [], [], [], []
	X,Y,D=[],[],[]
	featfolders, labelfolders, datefolders = getFiles()
	for i in listerr:
		print "Loading features for ", featfolders[i]
		ffold, lfold,dfold = featfolders[i], labelfolders[i], datefolders[i]
		d,x,y = splitData(ffold, lfold, dfold, lastf)
		D.extend(d)
		X.extend(x)
		Y.extend(y)	
	z=zip(D,X,Y)
	z.sort()
	D,X,Y=zip(*z)
	train_split=0.6
	X_train=X[:int(train_split*len(X))]
	X_test=X[int(train_split*len(X)):]
	y_train=Y[:int(train_split*len(Y))]
	y_test=Y[int(train_split*len(Y)):]
	
	X_train,y_train = filter(X_train,y_train)

	testX,testY=[],[]
        for i in xrange(int(((1-train_split)/train_split)*len(X_train))): 
		testX.append(X_test[i][1:])
		testY.append(y_test[i])
        X_test=testX
	y_test=testY
        if lastf=='n':
                xtrain, xtest = [], []
                for each in X_train: xtrain.append(each[:-1])
                for each in X_test: xtest.append(each[:-1])
                X_train, X_test = xtrain, xtest
	
	print "Length of data is "
	print "X_train: " + str(len(X_train))
	print "X_test: " + str(len(X_test))
	print "y_train: " + str(len(y_train))
	print "y_test: " + str(len(y_test))
	#pickle.dump(X_train,open('X_train','wb'))
	#pickle.dump(X_test,open('X_test','wb'))
	#pickle.dump(y_train,open('y_train','wb'))
	#pickle.dump(y_test,open('y_test','wb'))
	
	xt=open('Xtrain.txt','w')
	t=0
	for datum in X_train:
		print>> xt, datum,t
		t+=1
	yt=open('Ytrain.txt','w')
	t=0
	for datum in y_train:
                print>> yt, datum,t
		t+=1
	xtt=open('Xtest.txt','w')
	t=0
        for datum in X_test:
                print>> xtt, datum,t
		t+=1
	ytt=open('Ytest.txt','w')
	t=0
        for datum in y_test:
                print>> ytt, datum,t
		t+=1
	
	return X_train, X_test, y_train, y_test
 
def train(X_train, y_train):
	clf = RandomForestClassifier(random_state=0, n_estimators=150).fit(X_train, np.ravel(y_train))
	#clf = svm.SVC(random_state=0).fit(X_train,np.ravel(y_train))
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
	print len(feat_names),len(feat)
	for i in range(numf):print imp[i][0], imp[i][1], feat_names[imp[i][1]]

def main(plotfileid):
	featfolders, _,_a = getFiles()
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

