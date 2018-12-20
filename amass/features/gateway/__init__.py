import abc
import amass
import logging
import re

import amass.features


class Gateway:
	"""
	Abstract class for defining a gateway data source
	"""
	__metaclass__ = abc.ABCMeta

	@abc.abstractmethod
	def __init__(self, config, cache):
		"""
		Abstract constructor.  Stores config and creates logger.

		:param config: AmassConfig object containing configuration information
		"""
		self.config = config
		self.cache = cache
		self.logger = logging.getLogger(self.__module__)

	@abc.abstractmethod
	def cache_history(self, startdate, enddate):
		"""
		Fetch data from specified gateway and store into cache.

		:param startdate: A string containing the start date in format
		"YYYY-MM-DD HH:MM:SS"
		:param enddate: A string containing the end date in format
		"YYYY-MM-DD HH:MM:SS"

		:return: True if successful and False otherwise
		"""
		raise NotImplementedError("Please implement cache_history method")


class FeatureSource(amass.features.FeatureSource):
	columns = { "JOB_ID": "VARCHAR(255) NOT NULL",
		"RESOURCE": "VARCHAR(100) NULL", "TOOL_NAME": "VARCHAR(100) NULL",
		"USER_SUBMIT_DATE": "DATETIME NULL",
		"REMOTE_JOB_ID": "VARCHAR(1023) NULL",
		"REMOTE_JOB_SUBMIT_DATE": "DATETIME NULL",
		"TERMINATE_DATE": "DATETIME NULL", "RESULT": "BOOLEAN",
		"ERROR_MSG": "LONGTEXT NULL",
		"USERNAME": "VARCHAR(100) NULL"}
	sorted_column_names = sorted(columns.keys())
	sorted_column_names.remove("JOB_ID")
	sorted_column_names.insert(0, "JOB_ID")

	def __init__(self, source_name, config, cache, gateway):
		"""
		Constructor method for Gateway specific feature sources

		:param config: AmassConfig object containing configuration information
		:param cache: A FeatureCache object used for this run.
		:param gateway: A string containing the name of gateway this feature
		source will be applied to.
		"""
		amass.features.FeatureSource.__init__(self, source_name, config, cache, gateway)
		self.logger.debug("Calling base GatewayFeatures constructor")
		config_errors = config.get_vars_by_regex(
			"gateway.%s" % gateway, "errors.")
		self.error_types = {}
		for error_type, error_string in config_errors.items():
			error_type = error_type.replace("errors.", "")
			self.error_types[error_type] = re.split("\s*,\s*", error_string)
		self.gateway = self.load_gateway(gateway)
		self.resources = self.gateway.get_resources()
		self.primary_keys = ["JOB_ID"]

	def _categorize_error(self, err, found_errors, unmatched_errors):
		"""
		Categorize the provided err into error subcategories if known; otherwise
		return as unmatched

		:param err: A string containing a job error message
		:param found_errors:  A hash array containing a running total of matched
		error messages
		:param unmatched_errors: A string array containing a list of unknown
		errors

		:return: True if matched; otherwise False
		"""
		for error_type in self.error_types:
			for error_sum in self.error_types[error_type]:
				if re.search(error_sum, err):
					if error_type not in found_errors:
						found_errors[error_type] = { }
					if error_sum not in found_errors[error_type]:
						found_errors[error_type][error_sum] = 0
					found_errors[error_type][error_sum] += 1
					return True
		unmatched_errors.append(err)
		return False

	def add_feature_resources(self, job_info):
		"""
		Adds the name of the resource the job executed on as a feature

		:param job_info: A job record from the cache

		:return: An array of length #resources where the job's resource is
		set to 1 and the rest of the values are 0.
		"""
		resource_col_index = self.sorted_column_names.index("RESOURCE")
		job_resource = job_info[resource_col_index]

		resource_feature = [0]*len(self.resources)
		resource_index = self.resources.index(job_resource)
		resource_feature[resource_index] = 1

		return resource_feature

	def add_feature_tools(self, job_info):
		"""
		Adds the name of the gateway tool the job used on as a feature

		:param job_info: A job record from the cache

		:return: An array of length #tools where the job's tool is
		set to 1 and the rest of the values are 0.
		"""
		tool_col_index = self.sorted_column_names.index("TOOL_NAME")
		tool = job_info[tool_col_index]

		tool_feature = [0]*len(self.gateway.get_tools())
		tool_feature[self.gateway.get_tools()[tool]] = 1

		return tool_feature

	def load_gateway(self, gateway):
		"""
		Create a Gateway object using the specified gateway class

		:param gateway:  A string containing the name of the gateway

		:return: A new Gateway object.
		"""
		classname = 'Gateway'
		gateway_class_name = 'amass.features.gateway.%s' % gateway
		module = __import__(gateway_class_name, fromlist=[classname])
		return getattr(module, classname)(self.config, self.cache)

	def list_feature_resources(self):
		"""
		List the names of resources

		:param job_info: A job record from the cache

		:return: An array of length #resources where the job's resource is
		set to 1 and the rest of the values are 0.
		"""
		return self.resources

	def list_feature_tools(self):
		"""
		List the names of these tools

		:param job_info: A job record from the cache

		:return: An array of length #tools where the job's tool is
		set to 1 and the rest of the values are 0.
		"""
		return self.gateway.get_tools()

	def cache_history(self, startdate, enddate):
		"""
		Fetch the specified history data from the gateway and cache it

		:param startdate: A string containing the start date in format
		"YYYY-MM-DD HH:MM:SS"
		:param enddate: A string containing the end date in format
		"YYYY-MM-DD HH:MM:SS"

		:return:
		"""
		if self.cache.has_source(str(self.gateway)):
			self.logger.info("Using cached data for %s" % self.gateway)
			return

		self.cache.create_source(str(self.gateway), self.columns, self.primary_keys)
		self.gateway.cache_history(startdate, enddate)

	def get_cache_info(self):
		"""
		Return summary info about this source info in the cache

		:return: A tuple containing the name, min date, and max date
		"""
		db_cursor = self.cache.query_source(
			str(self.gateway),
			["count(*)", "min(USER_SUBMIT_DATE)", "max(USER_SUBMIT_DATE)"])
		(count, min_date, max_date) = db_cursor.fetchone()
		return str(self.gateway), str(count), str(min_date), str(max_date)

	@staticmethod
	def get_job_field(jobinfo, fieldname):
		"""
		Fetch desired field from db_cursor

		:param jobinfo: An array of fields representing a returned row from a
		database query
		:param fieldname: The field that you want returned

		:return:  The specified field in job information
		"""
		field_index = 0
		try:
			field_index = FeatureSource.sorted_column_names.index(fieldname)
		except:
			return None
		return jobinfo[field_index]

	def list_errors(self, errors):
		"""
		Categorize the found errors into gateway error_types

		:param errors: A list of job errors

		:return: A 2D string array containing the categorized errors where each
		row is tuple containing the error_type, error summary, and error count
		as well as an array of unmatched error messages.
		"""
		found_errors = {}
		unmatched_errors = []
		for err in errors:
			self._categorize_error(err, found_errors, unmatched_errors)

		table = []
		for error_type in sorted(found_errors.keys()):
			for err in sorted(found_errors[error_type].keys()):
				row = [error_type, err, str(found_errors[error_type][err])]
			table.append(row)

		return table, unmatched_errors

	def query_job(self, job_id):
		"""
		Return the list of historical gateway jobs

		:return:  a db_cursor object
		"""
		db_cursor = self.cache.query_source(str(self.gateway), self.sorted_column_names, "JOB_ID = '%s'" % job_id)
		return db_cursor.fetchone()

	def query_jobs(self):
		"""
		Return the list of historical gateway jobs

		:return:  a db_cursor object
		"""
		return self.cache.query_source(str(self.gateway), self.sorted_column_names)
