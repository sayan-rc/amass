import ConfigParser
import dateutil.parser
import dateutil.tz
import logging
import re
import sys

logger = None

__all__ = ["features", "commands", "predict"]


def abort(error):
	"""
	Print out error message and cause program to exit with error code

	:param error: Text of error message

	:return: **Does not return**
	"""
	logger.error(error)
	sys.exit(1)


def config_logging(loglevel="INFO", logfile=None):
	"""
	Configure the logger for calling program.  If logfile is None, messages
	will be printed to stdout.

	:param loglevel: Level of messages to be printed [default: INFO]
	:param logfile: Redirect messages to file if exists [default: None]

	:return:  The configured logger object
	"""
	logging.basicConfig(
		filename=logfile,
		format='%(asctime)s - %(name)s:%(lineno)d - %(levelname)s - %(message)s',
		level=getattr(logging, loglevel.upper()))
	global logger
	logger = logging.getLogger(sys.argv[0])
	return logging.getLogger(sys.argv[0])


def datetime2string(date_time):
	"""
	Converts datetime objects to local timezone and returns the string version.

	:param date_time: A datetime object with any timezone

	:return: A string containing a date timestamp
	"""
	return date_time.astimezone(dateutil.tz.tzlocal())


def get_command(args):
	"""
	Look for command in user's command-line arguments.

	:param argv: A string array of user's command line arguments

	:return: A subclass object of amass.command.Command or none if not found
	"""
	classname = "Command"
	for i in range(len(args), 0, -1):
		module_name = "amass.commands.%s" % ".".join(args[:i])
		module_name = module_name.rstrip(".")
		try:
			module = __import__(module_name, fromlist=[classname])
			return getattr(module, classname)(), args[i:]
		except:
			continue
	return None, None


def print_table(headers, rows):
	"""
	Print out information in a tabular format to stdout

	:param headers: A string array where each element is a column header
	:param rows: A 2D string array where each row is a row in the table

	:return:
	"""
	rows.insert(0, ['-'*(len(h)+1) for h in headers])
	rows.insert(0, ["%s:" % h for h in headers])
	col_rows = zip(*rows)
	max_col_vals = [max(col_rows[i], key=len) for i, row in enumerate(col_rows)]
	formats = ["{:<%d}" % len(max_col) for max_col in max_col_vals]
	format_string = "     ".join(formats)
	for i, row in enumerate(rows):
		print format_string.format(*row)


def string2datetime(date_string):
	"""
	Converts strings of YYYY-MM-DD HH:MM:SS to datetime object.  Assumes
	local timezone.

	:param date_string: A string containing a date

	:return: A datetime object
	"""
	date = dateutil.parser.parse(date_string)
	if date.tzinfo is None:
		date = date.replace(tzinfo=dateutil.tz.tzlocal())
	return date.astimezone(dateutil.tz.tzlocal())


def xml_tag_value(xml_obj, tag):
	"""
	Search for the specified tag in the provided xml object and return the value

	:param xml_obj: An o
	:param tag:
	:return:
	"""
	return xml_obj.getElementsByTagName(tag)[0].firstChild.data


class AmassConfig(ConfigParser.RawConfigParser):
	"""
	Convenience class for getting info from config file
	"""
	def __init__(self, **kwargs):
		"""
		Create AmassConfig object

		:param kwargs: Same as arguments for ConfigParser.RawConfigParser

		:return: new AmassConfig object
		"""
		ConfigParser.RawConfigParser.__init__(self, kwargs)
		self.logger = logging.getLogger(self.__module__)

	def get_db_vars(self, section):
		"""
		Read variables (formatted as db.*) for a database connection from
		the specified section of the config file.

		:param section:  The section header to read variables from.

		:return:  A dictionary containing the db connection variable names
		and values.  The names have been stripped of the "db." prefixes.
		"""
		self.logger.debug('Reading database params from section %s' % section)
		variables = self.get_vars_by_regex(section, "^db\.(\S+)")
		if 'port' in variables:
			variables['port'] = int(variables['port'])
		return variables

	def get_vars_by_regex(self, section, var_regex):
		"""
		Read variables matching the specified regex from the specified section
		of the config file.

		:param section:  The section header to read variables from.
		:param var_regex: A string containing a regex

		:return: A dictionary containing matching variable names and values.  If
		the regex contains a group, the value of the captured group is used as
		the variable name.
		"""
		variables = {}
		for (name, value) in self.items(section):
			match = re.search(var_regex, name)
			if match:
				if len(match.groups()) == 1:
					name = match.group(1)
				variables[name] = value
		return variables

	def get_resources(self, section, quoted=False):
		"""
		Read resource information from the specified section of the config file.

		:param section:  The section header to read variables from.
		:param quoted: Add quotes around each resource name

		:return:  An array with each element containing the name of a resource
		"""
		resources = re.split("\s*,\s*", self.get(section, "resources"))
		if quoted:
			return ",".join(["'%s'" % x for x in resources])
		else:
			return resources
