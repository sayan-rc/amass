import logging
import numpy as np
from sklearn.ensemble import RandomForestClassifier
import time


class Prediction:
	TRAIN_TEST_SPLIT = 0.6

	def __init__(self):
		self.logger = logging.getLogger(self.__module__)

	def train(self, features, results):
		self.logger.info("Training included %d features and %d results" % (
			len(features), len(results)))
		start = time.clock()
		classifier = RandomForestClassifier(random_state=0, n_estimators=150).fit(
			features, np.ravel(results))
		self.logger.info("Training time took %d secs" % (time.clock() - start))
		return classifier

		# start = time.clock()
		# print "Start time " + str(start)
		# test(clf, X_train, X_test, y_train, y_test,
		# 	 "roc-" + s + "-" + a + "-" + plotfileid)
		# end = time.clock()
		# print "End time " + str(end)
		# print "Testing time took " + str(time.clock() - start)
		# print "Testing included " + str(len(X_test)) + " x " + str(
		# 	len(y_test))
		# printTopFeatures(clf, 10)