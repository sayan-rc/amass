import logging
import re

import pymysql

import amass


class FeatureCache:
	"""
	We cache data from our feature sources into a local SQL db for
	convenience.  This class manages data being inserted and queried from that
	cache.
	"""
	def __init__(self, config):
		"""
		Constructor for FeatureCache.

		:param config: AmassConfig object containing config info for local
		SQL db cache.

		:return: A new FeatureCache object.
		"""
		self.cache = {}
		self.config = config
		self.db_conn = None
		self.logger = logging.getLogger(self.__module__)
		self.cache_prefix = "amass"
		self.name = "cache"
		try:
			self.logger.debug('Reading db params for cache')
			self.db = config.get_db_vars(self.name)
		except Exception as e:
			amass.abort('Problem reading "cache" section in config file: %s' % (str(e)))
		try:
			self.logger.debug('Reading resource params for cache')
			self.resources = config.get_resources(self.name)
		except Exception as e:
			amass.abort('Problem reading "cache" section in config file: %s' % (str(e)))
		self.load()

	def _execute_sql(self, sql, if_error):
		"""
		Execute the specified query

		:param sql: An SQL query to execute
		:param if_error: An error message to print out if problem with query

		:return: A DB cursor if successful; otherwise None
		"""
		try:
			self.logger.debug(sql)
			db_cursor = self.db_conn.cursor()
			db_cursor.execute(sql)
			return db_cursor
		except Exception as e:
			self.logger.error("%s: %s" % (if_error, str(e)))
			return None

	def create_features(self, features_name, gateway_name, columns, keys):
		"""
		Store a new feature source into the cache.

		:param features_name: A string containing the name of the feature def
		:param gateway_name A string containing the name of the gateway
		:param columns: A hash array where each key is the name of a column for
		the new table and the value is the SQL spec for the column.
		:param keys: A list of column names to be used for the table's primary
		keys

		:return:  True if successful and False if not successful.
		"""
		tablename = self.features_to_table_name(features_name, gateway_name)
		return self.create_table(columns, keys, tablename)

	def create_source(self, source_name, columns, keys):
		"""
		Store a new feature source into the cache.

		:param source_name: A string containing the name of the feature source
		:param columns: A hash array where each key is the name of a column for
		the new table and the value is the SQL spec for the column.
		:param keys: A list of column names to be used for the table's primary
		keys

		:return:  True if successful and False if not successful.
		"""
		tablename = self.source_to_table_name(source_name)
		return self.create_table(columns, keys, tablename)

	def create_table(self, columns, keys, tablename):
		columnspec = ",\n\t".join(
			["%s %s" % (n, s) for n, s in columns.items()])
		keyspec = ""
		if keys is not None:
			keyspec = ", UNIQUE (%s)" % (",".join(keys))
		tablespec = "\nCREATE TABLE %s (\n\t%s%s)" % (
			tablename, columnspec, keyspec)
		self.logger.debug(
			"Creating cache table %s, %s" % (tablename, tablespec))
		try:
			self.db_conn.cursor().execute(tablespec)
			self.db_conn.commit()
			return True
		except:
			return False

	def delete(self, table_name, cond):
		"""
		Insert values into specified table.

		:param table_name: A string containing the name of the table
		:param cond: A string containing the condition for the values to delete
		from the table.

		:return:
		"""
		delete = "DELETE FROM %s where %s" % (table_name, cond)
		self.logger.debug(delete)
		self.db_conn.cursor().execute(delete)
		self.db_conn.commit()

	def drop_features(self, features_name, gateway_name):
		"""
		Remove the specified feature source from the cache.

		:param features_name: A string containing the name of the feature source
		:param gateway_name: A string containing the name of the gateway

		:return: True if successfully removed otherwise False.
		"""
		tablename = self.features_to_table_name(features_name, gateway_name)
		sql = "DROP TABLE %s" % tablename
		try:
			self.db_conn.cursor().execute(sql)
			self.db_conn.commit()
			return True
		except Exception as e:
			self.logger.error("Problem dropping source: %s" % str(e))
			return False

	def drop_source(self, source_name):
		"""
		Remove the specified feature source from the cache.

		:param source_name: A string containing the name of the feature source

		:return: True if successfully removed otherwise False.
		"""
		tablename = self.source_to_table_name(source_name)
		sql = "DROP TABLE %s" % tablename
		try:
			self.db_conn.cursor().execute(sql)
			self.db_conn.commit()
			del self.cache[tablename]
			return True
		except Exception as e:
			self.logger.error("Problem dropping source: %s" % str(e))
			return False

	def features_to_table_name(self, features, gateway):
		"""
		Get the name of the SQL table name for the specified feature set and
		gateway.

		:param features: A string containing the name of the feature set
		:oaran gateway: A string containing the gateway

		:return: A string containing the SQL table name for specified cached
		features
		"""
		return "%s_features_%s_%s" % (self.cache_prefix, features, gateway)

	def get_cached_features(self):
		"""
		Return the list of of cached features

		:return: An array of tuples where first element is the name of the
		feature set and the second is the gateway
		"""
		features = []
		feature_pattern = "%s_features_" % self.cache_prefix
		for cache_name in sorted(self.cache.keys()):
			if re.search(feature_pattern, cache_name):
				cache_name = cache_name.replace(feature_pattern, "")
				features.append(cache_name.split("_"))
		return features

	def get_cached_sources(self):
		"""
		Return the list of of cached sources

		:return: An array of tuples where first element is the name of the
		feature set and the second and third is the max and min dates
		"""
		sources = []
		source_pattern = "%s_source_" % self.cache_prefix
		for cache_name in sorted(self.cache.keys()):
			if re.search(source_pattern, cache_name):
				cache_name = cache_name.replace(source_pattern, "")
				sources.append(cache_name.split("_"))
		return sources

	def get_resource(self, resource_id):
		"""
		Get the cache resource name for provided resource id

		:param resource_id: An integer pointing to the cache name of the
		provided resource id

		:return: A string containing the cache name of provided resource
		"""
		return self.resources[resource_id]

	def get_source_name(self, table_name):
		"""
		Get the source name for the specified cached table.

		:param table_name: The name of a SQL table in the specified cache.

		:return: A string containing the name of the feature source
		"""
		source_prefix = "%s_%s" % (self.cache_prefix, "source")
		table_name = table_name.replace(source_prefix, "")
		return table_name.replace("_", ".")

	def has_features(self, features_name, gateway_name):
		"""
		Verify if specified source is cached.

		:param features_name: A string containing the name of the feature source

		:return: True if source is cached and False otherwise
		"""
		table_name = self.features_to_table_name(features_name, gateway_name)
		return table_name in self.cache

	def has_source(self, source_name):
		"""
		Verify if specified source is cached.

		:param source_name: A string containing the name of the feature source

		:return: True if source is cached and False otherwise
		"""
		table_name = self.source_to_table_name(source_name)
		return table_name in self.cache

	def insert(self, table_name, values):
		"""
		Insert values into specified table.

		:param table_name: A string containing the name of the table
		:param values: A hash array where the keys are the column names and the
		values are the column values.

		:return:
		"""
		columns = ", ".join(sorted(values))
		symbols = ", ".join(["%s"] * len(values))
		insert = "INSERT INTO %s (%s) VALUES (%s)" % (
			table_name, columns, symbols)
		sorted_vals = [values[name] for name in sorted(values)]
		self.logger.debug(insert)
		self.logger.debug(sorted_vals)
		self.db_conn.cursor().execute(insert, sorted_vals)
		self.db_conn.commit()

	def load(self):
		"""
		Read the local cache and find out what data sources are cached and
		if needed, refresh the data.

		:return: Returns true if successful.  If error, will force abort.
		"""
		self.db_conn = pymysql.connect(**self.db)
		self.db_conn.autocommit(True)
		sql = "SHOW TABLES"
		db_cursor = self.db_conn.cursor()
		db_cursor.execute(sql)
		for (dbtable,) in db_cursor:
			if re.search(self.cache_prefix, dbtable):
				self.cache[dbtable] = 1

	def query_aggregate(self, source_name, columns, aggregates, groups, cond):
		"""
		Run an aggregate function on the source table.  Runs an inner join
		to get all columns that match the aggregate function.

		:param source_name: A string containing the name of the feature source
		:param columns: An array where each string is the name of a column to
		return
		:param aggregates: A hash array where the key is the column name and
		the value is the aggregate function.
		:param groups:  An array of column names to run the aggregate against.
		:param cond: A string containing the condition to query_source

		:return:
		"""
		cols = ["%s(%s) as %s" % (f, col, col) for col, f in aggregates.items()]
		cols.extend(groups)
		inner_table = "SELECT %s FROM %s WHERE %s GROUP BY %s" % (
			", ".join(cols),
			self.source_to_table_name(source_name),
			cond,
			", ".join(groups)
		)
		sql = "SELECT %s FROM %s INNER JOIN (%s) as %s ON (%s AND %s)" % (
			", ".join(["T1.%s" % col for col in columns]),
			"%s as %s" % (self.source_to_table_name(source_name), "T1"),
			inner_table,
			"T2",
			" AND ".join(["T1.%s=T2.%s" % (col, col) for col in groups]),
			" AND ".join(["T1.%s=T2.%s" % (col, col) for col in aggregates])
		)
		return self._execute_sql(
			sql,
			"Problems running an aggregate query on source %s" % source_name)

	def query_features(self, features_def, gw_name, cols, addtl_cond="", order=None):
		"""
		Query the cached features.

		:param features_def: A string containing the feature set definition to
		query
		:param gw_name: A string containing the name of the gateway to query
		:param cols: A string array containing the column names to return
		:param addtl_cond: A string containing an extra SQL conditions
		:param order: A string array containing a list of column names

		:return: A db_cursor to the query results
		"""
		table = self.features_to_table_name(features_def, gw_name)
		gateway_table = self.source_to_table_name("gateway.%s" % gw_name)
		if addtl_cond:
			addtl_cond = "AND %s" % addtl_cond
		cond = "%s.GATEWAY_JOB_ID = %s.JOB_ID %s" % (table, gateway_table, addtl_cond)
		order_by = ""
		if order and len(order) > 0:
			order_by = "ORDER BY %s" % ", ".join(order)
		sql = "SELECT %s from %s, %s WHERE %s %s" % (
			", ".join(cols), table, gateway_table, cond, order_by)
		return self._execute_sql(
			sql,
			"Problems querying errors for %s, %s" % (features_def, gw_name))

	def query_source(self, source_name, columns, cond=""):
		"""
		Query the specified feature source.

		:param source_name: A string containing the name of the feature source
		:param columns:  A string array containing the column names to select.
		:param cond: A string containing SQL conditions

		:return: A db cursor containing the result of the query_source if successful;
		otherwise returns None.
		"""
		if cond != "":
			cond = " WHERE %s" % cond
		sql = "SELECT %s from %s %s" % (
			", ".join(columns), self.source_to_table_name(source_name), cond)
		self.logger.debug("Querying jobs, %s", sql)
		return self._execute_sql(sql,
			"Problems querying source %s:" % source_name)

	def source_to_table_name(self, source_name):
		"""
		Get the name of the SQL table name for the specified source.

		:param source_name: A string containing the name of the feature source

		:return: A string containing the SQL table name for specified source.
		"""
		return "%s_source_%s" % (self.cache_prefix, source_name.replace(".", "_"))