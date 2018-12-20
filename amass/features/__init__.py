import abc
import json
import logging
import re
import sys

import amass
from amass.features.cache import FeatureCache


class Feature:
	def __init__(self, name):
		self.name = name

	@abc.abstractmethod
	def list(self):
		pass

	@abc.abstractmethod
	def generate(self):
		pass


class Features:
	""""
	Convenience class for managing a complete set of enabled features for a
	particular science gateway
	"""

	def __init__(self, client_config, server_config):
		"""
		Constructor to create a new object for managing multiple features from
		multiple feature sources.

		:param client_config: AmassConfig object containing config info
		"""
		self.client_config = client_config
		self.server_config = server_config
		self.sources = {}
		self.features = {}
		self.logger = logging.getLogger(self.__module__)
		self.cache = FeatureCache(client_config)

		self.columns = {
			"GATEWAY_JOB_ID": "VARCHAR(100)",
			"FEATURES": "JSON"
		}
		self.column_names = sorted(self.columns.keys())
		self.primary_keys = ["GATEWAY_JOB_ID"]

		for (feature_set, feature_def) in self.client_config.items("features"):
			self.features[feature_set] = re.split("\s*,\s*", feature_def)

	def cache_init(self):
		"""
		Reads what data is in the current cache

		:return:
		"""
		self.cache.load()

	def cache_history(self, startdate, enddate):
		"""
		Fetch historical data from each feature source and cache it.

		:param startdate: A string containing the start date in format
		"YYYY-MM-DD HH:MM:SS"
		:param enddate: A string containing the end date in format
		"YYYY-MM-DD HH:MM:SS"

		:return:
		"""
		for source in self.features.values():
			source.cache_history(startdate, enddate)

	def generate_features(self, features_name, gateway_name, refresh=False):
		"""
		Generate the features from all configured feature sources.

		:return:
		"""
		cache_name = self.cache.features_to_table_name(features_name, gateway_name)
		gw = self.sources["gateway"]
		if self.cache.has_features(features_name, gateway_name):
			if refresh:
				self.cache.drop_features(features_name, gateway_name)
			else:
				amass.abort("Cached features already exists; " +
					"re-run with refresh if you want to re-generate them")
		self.cache.create_features(features_name, gateway_name, self.columns, self.primary_keys)
		db_cursor = gw.query_jobs()
		self.logger.info("Found %i gateway jobs" % db_cursor.rowcount)

		for job_info in db_cursor:
			ordered_features = self.generate_feature(features_name, job_info)
			entry = {
				"GATEWAY_JOB_ID": gw.get_job_field(job_info, "JOB_ID"),
				"FEATURES": json.dumps(ordered_features)
			}
			self.cache.insert(cache_name, entry)

		return True

	def generate_feature(self, features_name, job_info):
		self.logger.debug("Generating features for job %s" % job_info[0])
		features = {}
		for source in self.sources.values():
			self.logger.debug("Getting job features from source %s" % source)
			features.update(source.generate_features(job_info))
		ordered_features = []
		for feature in self.features[features_name]:
			self.logger.debug("Packing feature %s" % feature)
			ordered_features.extend(features[feature])
		return ordered_features

	def get_feature_source(self, source_name, gateway=None):
		"""
		Add the specified feature source by loading the appropriate class.

		:param source_name: A string containing the name of the feature source
		class
		:param gateway:  A string containing the name of the gateway
		:return:
		"""
		classname = 'FeatureSource'
		modulename = 'amass.features.%s' % source_name
		print modulename
		module = __import__(modulename, fromlist=[classname])
		feature_source_object = getattr(module, classname)(source_name, self.client_config, self.cache, gateway)
		return feature_source_object

	def get_features_results(self, feature_def, gw_name, filter_errors=[]):
		"""
		Get generated features from cache

		:param feature_def: A string containing the name of the feature
		definition
		:param gw_name: A string containing the name of the gateway
		:param filter_errors: An array of strings where each element is a
		an error to return.  Filters out errors not matching specified errors.

		:return:  A 2D array
		"""
		filter_cond = ""
		if filter_errors:
			filters = ["ERROR_MSG like '%%%s%%'" % err for err in filter_errors]
			filter_cond = "(%s OR ERROR_MSG is NULL)" % " OR ".join(filters)
		db_cursor = self.cache.query_features(
			feature_def, gw_name,
			["JOB_ID", "REMOTE_JOB_ID", "USER_SUBMIT_DATE", "RESULT", "FEATURES"],
			filter_cond, ["USER_SUBMIT_DATE"])
		self.logger.info("Found %d matching features" % db_cursor.rowcount)
		if db_cursor.rowcount == 0:
			return [[]], []

		features = []
		results = []
		for job_id, remote_job_id, submit_date, result, features_json in db_cursor:
			self.logger.debug(features_json)
			features.append(json.loads(features_json))
			results.append(result)

		return features, results

	def compare_to_old(self, feat_names, feature_def, job_id, old_value,
						submit_date):
		errors = []
		for j, feat in enumerate(feature_def):
			if feat != old_value["X"][j+1]:
				self.logger.error("%s %s" % (job_id, submit_date))
				self.logger.error(
					"Feat %d %s with value %s did not match old value %s" % (
						j, feat_names[j], str(feat),
						str(old_value["X"][j + 1])))
				errors.append(feat_names[j])
		if len(errors) > 0:
			self.logger.error("Found %d differences in features" % len(errors))

			for error in errors:
				if error.find("trestles") < 0:
					self.logger.error("Non trestles error found")
		else:
			self.logger.debug("Matched old features!")

	def get_gateway(self, gw_name):
		"""
		Return a gateway source object

		:param gw_name: A string containing the name of the gateway to return

		:return: An object of type FeatureSource
		"""
		return self.get_feature_source("gateway", gw_name)

	def list_cached_feature_errors(self, feature_def, gw_name, list_error=""):
		"""
		Retrieve error information from cached features.  If list_error is
		empty, returns a table summarizing the error categories, type, and count
		for specified feature definition and gateway.  If list_error is non-empty

		:param feature_def: A string containing the name of the feature
		definition
		:param gw_name: A string containing the name of the gateway
		:param list_error: A string array conta
		:return:
		"""
		gateway = self.get_feature_source("gateway", gw_name)
		db_cursor = self.cache.query_features(feature_def, gw_name, ["DISTINCT ERROR_MSG"], "RESULT=FALSE")
		errors = []
		for error, in db_cursor:
			errors.append(error.strip())
		if list_error:
			table = []
			for error in errors:
				if re.search(list_error, error):
					condensed_error = re.sub("\s+", " ", error)
					table.append([condensed_error])
			return table, None
		else:
			return gateway.list_errors(errors)

	def list_cached_features(self):
		"""
		Return the name of cached features

		:return: An array of tuples where first element is the name of the
		feature set and the second is the gateway
		"""
		feature_info = []
		for feature_set, gateway in self.cache.get_cached_features():
			db_cursor = self.cache.query_features(
				feature_set, gateway,
				["COUNT(*)", "MIN(USER_SUBMIT_DATE)", "MAX(USER_SUBMIT_DATE)"])
			(count, min_date, max_date) = db_cursor.fetchone()
			feature_info.append([
				feature_set, gateway, str(count), str(min_date), str(max_date)])
		return feature_info

	def list_cached_sources(self):
		"""
		Return the name of cached sources

		:return: An array of tuples where first element is the name of the
		sources and the second and third elements are the max and min dates
		"""
		sources_info = []
		for source_info in self.cache.get_cached_sources():
			print source_info
			source = self.get_feature_source(*source_info)
			sources_info.append(source.get_cache_info())
		return sources_info

	def list_features(self):
		"""
		List the names of cached features

		:return: A 2D table containing the feature names
		"""
		table = []
		for feature_set, feature_def in self.features.items():
			table.append([feature_set, ", ".join(feature_def)])
		return table

	def list_feature_names(self, features_def, gw_name):
		"""
		List the names of each feature in specified feature vector

		:param features_def: A string containing the name of the feature def
		:param gw_name: A string containing the name of the gateway

		:return: A string array containing each feature name in def
		"""
		feature_names = []
		for feature in self.features[features_def]:
			(source_name, feat_name) = feature.split(".")
			source = self.get_feature_source(source_name, gw_name)
			feature_names.extend(source.list_features(feat_name))
		return feature_names

	def load_or_fetch_sources(self, features_def, gw_name, start_date, end_date,
							  refreshes=[]):
		"""
		Load or fetch data from available sources to satisfy features in
		specified feature definition

		:param features_def: A string containing the name of a feature def
		:param gw_name: A string containing the name of the gateway
		:param start_date: A datetime object indicating the start date to fetch
		data if needed
		:param end_date: A datetime object indicating the end date to fetch
		data if needed
		:param refreshes: A string containing the name of any sources to refresh
		if already cached

		:return: True if sources were successfully loaded; otherwise False
		"""
		if features_def not in self.features:
			self.logger.error("Unable to find feature set %s" % features_def)
		self.logger.info("Adding feature sources")
		for feature in self.features[features_def]:
			feature_source, feature_name = feature.split(".")
			if feature_source not in self.sources:
				self.sources[feature_source] = self.get_feature_source(feature_source, gw_name)
			self.sources[feature_source].add_feature(feature_name)

		for source_name, source in self.sources.items():
			self.logger.debug("Looking at source %s in '%s'" % (source_name, ", ".join(refreshes)))
			self.logger.debug("%s" % self.cache.has_source(source_name))
			self.logger.debug("%s" % source_name in refreshes)
			if self.cache.has_source(source_name) and source_name in refreshes:
				self.logger.info(
					"Refreshing feature source %s" % source_name)
				if self.cache.drop_source(source_name):
					self.logger.info(
						"Dropped feature source table %s" % source)
				else:
					amass.abort(
						"Unable to drop feature source %s" % source)
			source.cache_history(start_date, end_date)

		return True


class FeatureSource:
	"""
	Abstract class used to manage diverse sources of data for machine learning
	features.
	"""
	__metaclass__ = abc.ABCMeta

	@abc.abstractmethod
	def __init__(self, source_name, config, cache, gateway=None):
		"""
		Constructor for Feature source.  Stores configuration object and
		initializes feature_names array and logger.

		:param config: AmassConfig object containing configuration information
		:param cache: A FeatureCache object used for this run.
		:param gateway: A string containing the name of gateway this feature
		source will be applied to.
		"""
		self.config = config
		self.name = source_name
		self.cache = cache
		self.feature_names = []
		self.logger = logging.getLogger(self.__module__)

	def __str__(self):
		return self.name

	def add_feature(self, feature_name):
		"""
		Enable specified feature from FeatureSource

		:param feature_name: A string containing the name of feature
		"""
		self.logger.info('Adding feature "%s"' % feature_name)
		self.feature_names.append(feature_name)

	@abc.abstractmethod
	def cache_history(self, startdate, enddate):
		"""
		Fetch data from specified feature source and store into cache.

		:param startdate: A string containing the start date in format
		"YYYY-MM-DD HH:MM:SS"
		:param enddate: A string containing the end date in format
		"YYYY-MM-DD HH:MM:SS"

		:return: True if successful and False otherwise
		"""
		raise NotImplementedError("Please implement cache_history method")

	def generate_features(self, job_info):
		"""
		Generate the features for specified job.

		:param job_info: A job record from the cache

		:return: A 2D array where a row represent a feature and the columns
		are for each job
		"""
		features = {}
		for feature in self.get_feature_names():
			feature_function = "add_feature_%s" % feature
			self.logger.debug("Adding feature %s" % feature)
			try:
				features["%s.%s" % (self.name, feature)] = getattr(self, feature_function)(job_info)
			except Exception as e:
				self.logger.error("Unable to add feature %s: %s" % (feature, str(e)))
		return features

	def get_feature_names(self):
		"""
		Return the list_cached_features of enabled feature names for this feature source

		:return: A string array where each item is an enabled feature
		"""
		return self.feature_names

	@abc.abstractmethod
	def get_cache_info(self):
		"""
		Return summary info about this source info in the cache

		:return: A tuple containing the name, min date, and max date
		"""
		raise NotImplementedError("Please implement cache_history method")

	def list_features(self, feature_name):
		"""
		Generate the features for specified job.

		:param feature_name: A string containing the name of the feature def

		:return: A string array containing the name of the features in this
		definition
		"""
		feature_function = "list_feature_%s" % feature_name
		try:
			return getattr(self, feature_function)()
		except Exception as e:
			self.logger.error("Unable to list_cached_features feature %s: %s" % (feature_name, str(e)))
		return []