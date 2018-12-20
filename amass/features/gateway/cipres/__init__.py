import amass
import pickle
import pymysql
from string import Template


class Gateway(amass.features.gateway.Gateway):
	"""
	Implements data source for CIPRES gateway
	"""
	def __init__(self, config, cache):
		"""
		Create CIPRES gateway data source object

		:param config: AmassConfig object containing configuration information
		"""
		amass.features.gateway.Gateway.__init__(self, config, cache)
		self.name = "gateway.cipres"
		self.db_conn = None
		try:
			self.logger.info('Reading db params for CIPRES database')
			self.db = config.get_db_vars(self.name)
		except Exception as e:
			amass.abort('Problem reading db params from "%s" section in config file: %s' % (self.name, str(e)))
		try:
			self.resources_as_string = config.get_resources(self.name, True)
			self.resources = config.get_resources(self.name)
		except:
			amass.abort('Problem finding "resources" in "%s" section in config file')
		# TODO get rid of pickle file
		self.tools = pickle.load(open("data/tool_ids.pkl", "rb"))
		self.logger.debug("Read %d tools from file" % len(self.tools))


	def __str__(self):
		"""
		Return the name of this gateway

		:return: A string containing the name of the gateway
		"""
		return self.name

	def _get_resource_name(self, resource):
		"""
		Convert the resource name as stored in the CIPRES database to an
		XSEDE resource name.

		:param resource:  A string containing the name of the CIPRES resource

		:return: A string containing the XSEDE name of the resource.
		"""
		return resource.lower()

	def cache_history(self, startdate, enddate):
		"""
		Fetch data from CIPRES gateway and store into cache.  Renames CIPRES
		columns to generic gateway columns.

		:param startdate: A string containing the start date in format
		"YYYY-MM-DD HH:MM:SS"
		:param enddate: A string containing the end date in format
		"YYYY-MM-DD HH:MM:SS"

		:return: True if successful and False otherwise
		"""
		self.db_conn = pymysql.connect(**self.db)

		# get jobs data from CIPRES
		query_tmpl = """
			SELECT  job_stats.jobhandle, resource, tool_id, date_entered,
				date_terminated, date_submitted, name, remote_job_id, value, username
			FROM job_stats, job_events, users
			WHERE
				(job_stats.jobhandle=job_events.jobhandle) AND
				(job_stats.user_id = users.user_id) AND
				(resource in ($resources)) AND
				(job_events.name in ('FINISHED', 'FAILED')) AND
				(date_entered >= '$startDate') AND
				(date_entered <= '$endDate')
		"""
		sql = Template(query_tmpl).substitute(
			startDate=startdate,
			endDate=enddate,
			resources=self.resources_as_string)
		db_cursor = self.db_conn.cursor()
		self.logger.debug("Executing query_source: %s" % sql)
		db_cursor.execute(sql)
		self.logger.info(
			"Query returned %s CIPRES job records" % db_cursor.rowcount)
		num_records = 0
		for (
				jobhandle, resource, tool_id, date_e, date_t, date_s, name,
				remote_job_id, value, username) in db_cursor:
			# TODO: need to mark failures as not failures,e.g., too large or
			# remove nonjobs e.g., deleted task
			values = {
				"JOB_ID": jobhandle, "TERMINATE_DATE": date_t,
				"REMOTE_JOB_ID": remote_job_id,
				"RESOURCE": self._get_resource_name(resource),
				"RESULT": True if name == 'FINISHED' else False,
				"REMOTE_JOB_SUBMIT_DATE": date_s, "ERROR_MSG": value,
				"TOOL_NAME": tool_id, "USER_SUBMIT_DATE": date_e,
				"USERNAME": username}
			try:
				self.cache.insert(self.name, values)
				num_records += 1
			except Exception as e:
				self.logger.error(
					"Unable to insert job record with jobhandle %s: %s" % (
						jobhandle, str(e)))

		self.logger.info("Inserted %i job records into cache" % num_records)

	def get_resources(self):
		"""
		Return the XSEDE names of resources used in CIPRES database.

		:return: An array of strings containing the XSEDE resource names
		"""
		resources = []
		for resource in self.resources:
			resources.append(self._get_resource_name(resource))
		return resources

	def get_tools(self):
		"""
		Return the list of CIPRES tools

		:return: An array of strings containing CIPRES tool names
		"""
		return self.tools
