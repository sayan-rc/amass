import pickle	
import pylab as pl
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

def print_metrics(clf, X_train, X_test, y_train, y_test, name):
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
	try:
		fpr, tpr, thresholds = roc_curve(y_test, clf.predict_proba(X_test)[:,1])
		#print clf.predict_proba(X_test)[0:30,:]
		#print clf.feature_importances_
		#print clf.get_params()
		#print thresholds, fpr, tpr
		plt.plot(fpr,tpr,'ro')
		plt.plot(fpr,tpr)
		plt.show()
	except:
		pred = clf.decision_function(X_test)
		fpr, tpr, thresholds = roc_curve(y_test, pred)
		for i in range(len(fpr)):
			#y_pred = pred>thresholds[i]
			#f = f1_score(y_test, y_pred,average=None)
			if(1-fpr[i]>0.4 and tpr[i]>.98): print fpr, tpr
	roc_auc = auc(fpr,tpr)
	pl.clf()
	pl.plot(fpr, tpr, label='ROC curve (area = %0.2f)' % roc_auc)
	pl.xlabel('False Positive Rate')
	pl.ylabel('True Positive Rate')
	pl.title(name)
	pl.legend(loc="lower right")
	pl.show()
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
	
		
scale = False
matX = pickle.load(open('/Users/kritika/Courses/Project/XDCDB/amass/data/errsubX.pkl','rb'))
matY = pickle.load(open('/Users/kritika/Courses/Project/XDCDB/amass/data/errsubY.pkl','rb'))
X_train, X_test, y_train, y_test = cross_validation.train_test_split(matX, matY, test_size=0.4, random_state=0)
if(scale):
	scaler = StandardScaler(copy=False).fit(X_train)
	scaler.transform(X_train)
	scaler.transform(X_test)
'''
pca = PCA()
pca.fit(matX)
print pca.explained_variance_ratio_
print list(pca.components_[0]), list(pcs_components_[1])


#Linear SVC with l2 penalty, dual=True
print "Linear SVM with l2, dual=true"
clf = svm.LinearSVC(penalty='l2', dual=True, random_state=0).fit(X_train, np.ravel(y_train))
print_metrics(clf, X_train, X_test, y_train, y_test, "Linear SVM with l2 penalty")
#print(classification_report(y_test, clf.predict(X_test)))

#Linear SVC with l1 penalty
print "Linear SVM with l1"
clf = svm.LinearSVC(penalty='l1', dual=False, random_state=0).fit(X_train, np.ravel(y_train))
print_metrics(clf, X_train, X_test, y_train, y_test, "Linear SVM with l1 penalty")
#print(classification_report(y_test, clf.predict(X_test)))

#Linear SVC with l2 penalty
print "Linear SVM with l2"
clf = svm.LinearSVC(penalty='l2', dual=False, random_state=0).fit(X_train, np.ravel(y_train))
print_metrics(clf, X_train, X_test, y_train, y_test, "Linear SVM with l2 penalty")
#print(classification_report(y_test, clf.predict(X_test)))

#SVC with rbf
print "RBF"
clf = svm.SVC(kernel='rbf', random_state=0).fit(X_train, np.ravel(y_train))
print_metrics(clf, X_train, X_test, y_train, y_test, "SVM with rbf kernel")
#print(classification_report(y_test, clf.predict(X_test)))

#SVC with poly
print "Poly"
clf = svm.SVC(kernel='poly', random_state=0).fit(X_train, np.ravel(y_train))
print_metrics(clf, X_train, X_test, y_train, y_test, "SVM with polynomial kernel")

#SVC with sigmoid
print "Sigmoid"
clf = svm.SVC(kernel='sigmoid', random_state=0).fit(X_train, np.ravel(y_train))
print_metrics(clf, X_train, X_test, y_train, y_test, "Sigmoid")
'''
#Logistic regression with l1 penalty
print "Logreg with l1"
clf = LogisticRegression(penalty='l1').fit(X_train, np.ravel(y_train))
print clf.coef_
print_metrics(clf, X_train, X_test, y_train, y_test, "Logistic regression with l1 penalty")
#print(classification_report(y_test, clf.predict(X_test)))

#Logistic regression with l2 penalty
print "Logreg with l2"
clf = LogisticRegression(penalty='l2').fit(X_train, np.ravel(y_train))
print clf.coef_
print_metrics(clf, X_train, X_test, y_train, y_test, "Logistic regression with l2 penalty")
#print(classification_report(y_test, clf.predict(X_test)))
'''
#Nearest neighbors
print "Knn with k=1"
clf = KNeighborsClassifier(n_neighbors=1).fit(X_train, np.ravel(y_train))
print_metrics(clf, X_train, X_test, y_train, y_test, "Knn with k=1")
#print(classification_report(y_test, clf.predict(X_test)))

#Nearest neighbors
print "Knn with k=3"
clf = KNeighborsClassifier(n_neighbors=3).fit(X_train, np.ravel(y_train))
print_metrics(clf, X_train, X_test, y_train, y_test, "Knn with k=3")
#print(classification_report(y_test, clf.predict(X_test)))

#Nearest neighbors
print "Knn with k=5"
clf = KNeighborsClassifier(n_neighbors=5).fit(X_train, np.ravel(y_train))
print_metrics(clf, X_train, X_test, y_train, y_test, "Knn with k=5")
#print(classification_report(y_test, clf.predict(X_test)))
'''
#Gaussian Naive Bayes
print "Naive Bayes"
clf = GaussianNB().fit(X_train, np.ravel(y_train))
print_metrics(clf, X_train, X_test, y_train, y_test, "Naive Bayes")
#print(classification_report(y_test, clf.predict(X_test)))

#Decision tree classifier
print("Decision tree")
clf = DecisionTreeClassifier(random_state=0).fit(X_train, np.ravel(y_train))
print_metrics(clf, X_train, X_test, y_train, y_test, "Decision tree")
#print(classification_report(y_test, clf.predict(X_test)))

#Random forest classifier
print("Random forest")
clf = RandomForestClassifier(random_state=0).fit(X_train, np.ravel(y_train))
feat = list(clf.feature_importances_)
imp = []
for i in range(len(feat)):
	imp.append([feat[i],i])
imp.sort(reverse=True)
f = open('data/feat_names.pkl','rb')
feat_names = pickle.load(f)
for i in imp:
	print i
	print feat_names[i[1]]
print_metrics(clf, X_train, X_test, y_train, y_test, "Random forest")
#print(classification_report(y_test, clf.predict(X_test)))

