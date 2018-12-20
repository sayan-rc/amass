import amass
import amass.features
import amass.features.gateway
import bisect
import dateutil.parser
import dateutil.tz
import glob
import os
import sys
import test
import xml.dom.minidom


class FeatureSource(amass.features.FeatureSource):
	def __init__(self, source_name, config, cache, gateway=None):
		"""
		Create Inca feature source object.

		:param config: AmassConfig object containing configuration information
		:param cache: A FeatureCache object used for this run.
		:param gateway: A string containing the name of gateway this feature
		source will be applied to.
		"""
		amass.features.FeatureSource.__init__(
			self, source_name, config, cache, gateway)
		self.cache_table_name = self.cache.source_to_table_name(self.name)
		self.resources = {}
		for i, resource in enumerate(config.get_resources(self.name)):
			self.resources[resource] = i
		self.history_path = config.get("inca", "dir")

		self.columns = {
			"SOURCE_RESOURCE": "VARCHAR(100)",
			"TARGET_RESOURCE": "VARCHAR(100)",
			"TEST_NAME": "VARCHAR(100)",
			"COLLECTED_DATE": "DATETIME",
			"RESULT": "BOOLEAN",
			"ERROR_MSG": "LONGTEXT NULL"
		}
		self.column_names = sorted(self.columns.keys())
		self.primary_keys = [
			"SOURCE_RESOURCE", "TARGET_RESOURCE", "TEST_NAME", "COLLECTED_DATE"]
		self.test_names = sorted(test.test_names)

	def _cache_test(self, test, startdate, enddate):
		# TODO pad start time
		self.logger.debug("Caching inca test %s" % test)
		results = sorted(os.listdir(test))
		if len(results) < 1:
			self.logger.debug("  No test results found...skipping")
			return
		results_ts = [dateutil.parser.parse(result) for result in results]
		self.logger.debug("  Test lifetime from %s to %s" % (
			min(results_ts), max(results_ts)))
		normalized_start = amass.string2datetime(startdate)
		normalized_end = amass.string2datetime(enddate)
		min_index = bisect.bisect_right(results_ts, normalized_start)
		max_index = bisect.bisect_left(results_ts, normalized_end)
		relevant_results = results[min_index:max_index]
		if len(relevant_results) < 1:
			if results_ts[-1] > normalized_start:
				self.logger.debug("  Test is out of scope")
				return
			# TODO: remove this -- just for backwards compatibility testing
			relevant_results = [results[-1]]
		self.logger.debug("  Trimmed test lifetime from %s to %s" % (
				min(relevant_results), max(relevant_results)))
		num_cached = 0
		for result in relevant_results:
			result_file = os.path.join(test, result)
			self.logger.debug("  Reading result file %s" % result_file)
			result = self._get_result(result_file)
			self.logger.debug(
				"Trying to insert values '%s'" % (str(result)))
			try:
				self.cache.insert(self.cache_table_name, result)
				num_cached += 1
				self.logger.debug("Successfully inserted values '%s'" % (
					str(result)))
			except Exception as e:
				self.logger.error("Problems inserting values '%s': %s" % (
					str(result), str(e)))
		self.logger.info("  Inserted %i results into cache" % num_cached)

	def _get_resource_test_names(self):
		"""
		Find the names of tests relevant to configured resources.

		:return:  A hash array where the inca resource name is the key and the
		value is an array of test names.
		"""
		tests = []
		for resource in self.resources:
			path = os.path.join(self.history_path, '*', '*', resource)
			self.logger.info("Looking for Inca tests in %s" % path)
			tests.extend(glob.glob(path))
		return tests

	def _get_result(self, test_result_file):
		"""
		Interpret the success or failures for specified test report.  If a
		comparitor exists, check that result.  Otherwise, get result from
		completed tag.

		:param test_result_file: File containing Inca XML test report

		:return: Tuple where first element is true/false indicating test failure
		and second is error message if false otherwise None
		"""
		result = {}

		xml_obj = xml.dom.minidom.parse(test_result_file)
		if not xml_obj.getElementsByTagName("completed"):
			sys.stderr.write(
				"File has empty result...removing %s\n" % test_result_file)
			os.remove(test_result_file)
			return

		inca_resource = amass.xml_tag_value(xml_obj, "resourceHostname")
		result["SOURCE_RESOURCE"] = self._normalize_resource(inca_resource)
		try:
			inca_resource = amass.xml_tag_value(xml_obj, "targetHostname")
			result["TARGET_RESOURCE"] = self._normalize_resource(inca_resource)
		except:
			result["TARGET_RESOURCE"] = result["SOURCE_RESOURCE"]
		result["TEST_NAME"] = amass.xml_tag_value(xml_obj, "nickname")
		result["COLLECTED_DATE"] = amass.string2datetime(amass.xml_tag_value(xml_obj, "gmt"))
		result["RESULT"] = None
		error = None

		try:
			error = amass.xml_tag_value(xml_obj, "errorMessage")
		except:
			pass
		try:
			cr = amass.xml_tag_value(xml_obj, "comparisonResult")
			if cr == 'Success':
				result["RESULT"] = True
			else:
				error = cr if error is None else "%s: %s" % (cr, error)
				result["RESULT"] = False
		except:
			completed = amass.xml_tag_value(xml_obj, "completed")
			if completed == 'true':
				result["RESULT"] = True
			else:
				result["RESULT"] = False

		if error:
			error.replace("'", "")
		result["ERROR_MSG"] = error

		return result

	def _normalize_resource(self, inca_resource):
		if inca_resource not in self.resources:
			return inca_resource
		return self.cache.get_resource(self.resources[inca_resource])

	def add_feature_tests(self, job_info):
		job_date = amass.features.gateway.FeatureSource.get_job_field(job_info, "USER_SUBMIT_DATE")

		# TODO: filter out old results -- maintaining for backwards compat.
		db_cursor = self.cache.query_aggregate(
			self.name,
			self.column_names,
			{'COLLECTED_DATE': 'max'},
			["SOURCE_RESOURCE", "TARGET_RESOURCE", "TEST_NAME"],
			"COLLECTED_DATE <= '%s'" % str(job_date)
		)
		result_feature = {}
		self.logger.debug("Found %d matching tests" % db_cursor.rowcount)
		for row in db_cursor:
			print row
			result = {}
			for i, field in enumerate(self.column_names):
				result[field] = row[i]
			resource = result["SOURCE_RESOURCE"] if result["TARGET_RESOURCE"] == '' else result["TARGET_RESOURCE"]
			old_key = "data/%s/%s" % (resource, result["TEST_NAME"])
			self.logger.debug(
				"Setting test %s to %s" % (old_key, str(result["RESULT"])))
			result_feature[old_key] = result["RESULT"]

		# TODO: Remove adding of new results -- maintaining for backwards compat.
		db_cursor = self.cache.query_aggregate(
			self.name,
			self.column_names,
			{'COLLECTED_DATE': 'min'},
			["SOURCE_RESOURCE", "TARGET_RESOURCE", "TEST_NAME"],
			"COLLECTED_DATE > '%s'" % str(job_date)
		)
		self.logger.debug("Found %d newer tests" % db_cursor.rowcount)
		for row in db_cursor:
			result = {}
			for i, field in enumerate(self.column_names):
				result[field] = row[i]
			resource = result["SOURCE_RESOURCE"] if result["TARGET_RESOURCE"] == '' else result["TARGET_RESOURCE"]
			old_key = "data/%s/%s" % (resource, result["TEST_NAME"])
			if old_key not in result_feature:
				self.logger.debug("Setting new test %s to %s" % (old_key, str(result["RESULT"])))
				result_feature[old_key] = result["RESULT"]

		# TODO: remove really old features
		result_feature["data/gordon/ca-crl-check-5.0.0"] = 1
		result_feature["data/gordon/ca-tarball-version"] = 0
		result_feature["data/trestles/ca-crl-check-5.0.0"] = 1
		result_feature["data/trestles/hostcert-check-5.0.0"] = 1

		test_features = []
		for i, test_name in enumerate(self.test_names):
			if test_name in result_feature:
				test_features.append(result_feature[test_name])
			else:
				self.logger.debug("Artificially setting feature %s to True" % test_name)
				test_features.append(1)
			self.logger.debug(
				"Setting test feature %d %s to %s" % (i, test_name, str(test_features[-1])))

		return test_features

	def cache_history(self, startdate, enddate):
		"""
		Fetch the specified history data from the gateway and cache it

		:param startdate: A string containing the start date in format
		"YYYY-MM-DD HH:MM:SS"
		:param enddate: A string containing the end date in format
		"YYYY-MM-DD HH:MM:SS"

		:return:
		"""
		if not self.cache.has_source(self.name):
			self.cache.create_source(self.name, self.columns, self.primary_keys)
			tests = self._get_resource_test_names()
			self.logger.info("Found %i relevant inca tests" % len(tests))
			for test_name in tests:
				self._cache_test(test_name, startdate, enddate)
		else:
			self.logger.info("Using cached data for %s" % self.name)

	def get_cache_info(self):
		"""
		Return summary info about this source info in the cache

		:return: A tuple containing the name, count, min date, and max date
		"""
		db_cursor = self.cache.query_source(self.name,
			["count(*)", "min(COLLECTED_DATE)", "max(COLLECTED_DATE)"])
		(count, min_date, max_date) = db_cursor.fetchone()
		return self.name, str(count), str(min_date), str(max_date)

	def list_feature_tests(self):
		"""
		Return the list of test names

		:return: A string containing the names of tests
		"""
		return self.test_names
