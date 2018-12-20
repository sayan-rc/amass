from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt
import os
import pymysql
import sys
import time

HTMLDIR = "/var/www/html/amass/stats/"
scriptdir = os.path.dirname(os.path.realpath(__file__))
DATABASES = None
CSS = None
# configures DATABASES
execfile(os.path.join(scriptdir, "stats.conf"), {}, globals())

failedFinishedFields = {
	'resource': {'label': 'Resources', 'rotate': 0, 'threshold': 0, 'width': '500'},
	'tool_id': {'label': 'Tools', 'rotate': 90, 'threshold': 0, 'width': '500'},
	'username': {'label': 'Users', 'rotate': 90, 'threshold': 10, 'width': '750'}
}


def plot_count(feature_label, features, title, filename, **opt_args):
	"""
	Generate plot showing counts of a feature

	:param feature_label: Name of feature
	:param features:
	:param title:
	:param filename:
	:param opt_args:
	:return:
	"""
	xlabel_rotation = 0
	if "xlabel_rotation" in opt_args:
		xlabel_rotation = opt_args["xlabel_rotation"]
	threshold = 0
	if "threshold" in opt_args:
		threshold = opt_args["threshold"]
	print "Creating figure %s with threshold %d" % (filename, threshold)
	plt.clf()
	plt.figure().suptitle(title.upper())
	num_jobs = []
	xlabels = []
	feature_count = 0
	for feature in sorted(features.keys()):
		count = features[feature]['finished'] + features[feature]['failed']
		if count < threshold:
				continue
		feature_count += 1
		num_jobs.append(count)
		xlabels.append(feature)
	plt.ylabel('Number of jobs', fontsize='15')
	label = feature_label
	if threshold > 0:
		label += " (submitted at least %d jobs)" % threshold
	plt.xlabel(label, fontsize='15')
	plt.bar(np.arange(0, feature_count), num_jobs, width=.9)
	plt.xticks(np.arange(.5, feature_count), xlabels, rotation=xlabel_rotation)
	plt.tight_layout()
	fig = plt.gcf()
	newwidth = 6 + .25 * feature_count
	oldsize = fig.get_size_inches()
	fig.set_size_inches(newwidth, oldsize[1])
	plt.savefig(filename)
	fig.set_size_inches(oldsize[0], oldsize[1])


def plot_finished_failed(feature_label, features, filename, **opt_args):
	xlabel_rotation = 0
	if "xlabel_rotation" in opt_args:
		xlabel_rotation = opt_args["xlabel_rotation"]
	threshold = 0
	if "threshold" in opt_args:
		threshold = opt_args["threshold"]
	print "Creating figure %s with threshold %d" % (filename, threshold)

	plt.clf()
	plt.ylabel('Fraction of finished/failed jobs', fontsize=15)
	label = feature_label
	if threshold > 0:
		label += " (submitted at least %d jobs)" % threshold
	plt.xlabel(label, fontsize=15)
	failed, finished, xlabels = [], [], []
	feature_count = 0
	for feature in sorted(features.keys()):
		feature_total = features[feature]['finished'] + features[feature]['failed']
		if feature_total < threshold:
			continue
		feature_count += 1
		failed.append(float(features[feature]['failed']) / feature_total)
		finished.append(float(features[feature]['finished']) / feature_total)
		xlabels.append(feature)
	pfi = plt.bar(np.arange(0.1, feature_count, 1), finished, width=.8, color='b')
	pfa = plt.bar(np.arange(0.1, feature_count, 1), failed, bottom=finished, width=.8, color='0.75')
	plt.xticks(np.arange(0.5, feature_count, 1), xlabels, rotation=xlabel_rotation)
	plt.xlim([0, feature_count])
	plt.legend((pfi[0], pfa[0]), ('Finished', 'Failed'), loc='lower right')
	plt.tight_layout()
	fig = plt.gcf()
	newwidth = 6 + .25 * feature_count
	oldsize = fig.get_size_inches()
	fig.set_size_inches(newwidth, oldsize[1])
	plt.savefig(filename)
	fig.set_size_inches(oldsize[0], oldsize[1])


def plot_job_gap(gap):
	plt.figure()
	MIN, MAX = .0000001, 1000000
	bins = 10 ** np.linspace(np.log10(MIN), np.log10(MAX), 50)
	y, x = np.histogram(gap, bins=bins)
	plt.ylabel('Number of jobs')
	plt.xlabel('Time difference from the last job (in hours)')
	pf = plt.bar(np.arange(0, len(bins) - 1), y, color='b', width=1)
	plt.show()


def plot_jobduration_hist(hours, title, filename):
	plt.clf()
	plt.figure().suptitle(title.upper())
	plt.ylabel('Number of jobs', fontsize='15')
	plt.xlabel('Job turnaround time (in hours)', fontsize='15')
	plt.hist(hours, 25)
	plt.savefig(filename)
	plt.clf()


def plot_jobduration_percent_hist(failed, finished, title, filename):
	plt.clf()
	plt.figure().suptitle(title.upper())
	r = max(max(finished), max(failed))
	binsize = 5  # hours
	binsize_estimate = float(r) / 25
	if binsize_estimate > 5:
		binsize = 25
	bins = list(np.arange(0, r + binsize, binsize))
	yfi, xfi = np.histogram(finished, bins=bins)
	yfa, xfa = np.histogram(failed, bins=bins)
	y = yfi + yfa
	yfi = np.array(yfi, dtype=float) / np.array(y, dtype=float)
	yfa = np.array(yfa, dtype=float) / np.array(y, dtype=float)

	plt.ylabel('Fraction of finished/failed jobs', fontsize='15')
	plt.xlabel('Job turnaround time (in hours)', fontsize='15')
	pfi = plt.bar(np.arange(0, len(bins) - 1), yfi, color='b', width=1)
	pfa = plt.bar(np.arange(0, len(bins) - 1), yfa, bottom=yfi, color='0.75', width=1)
	plt.xticks(np.arange(len(bins)), bins)
	plt.legend((pfi[0], pfa[0]), ('Finished', 'Failed'), loc='lower right')
	plt.savefig(filename)


def plot_failure_bursts(x_dict, x_name, err):
	plt.figure()
	print max(x_dict.keys())
	x = sorted(x_dict.keys())
	y = [];


#
#
# for length in x:
# 	y.append( x_dict[length] )
# plt.xlabel( 'Burst length (hours)', fontsize=15 )
# plt.ylabel( 'Number of bursts', fontsize=15 )
# plt.title( x_name )
# # plt.title('Failure bursts on ' + x_name + ' due to error type ' + err)
# print x_dict.values( )
# print y
# print np.arange( len( x ) )
# print np.arange( len( x ) ) + 0.5
# pf = plt.bar( np.arange( len( x ) ), y, color='b', width=1 )
# plt.xticks( np.arange( len( x ) ) + 0.5, x, rotation=0, fontsize=7 )
# x1, x2, y1, y2 = plt.axis( )
# plt.axis( (x1, x2, y1, 120) )
# plt.savefig( 'bursts/' + x_name + err + '.eps' )
# plt.show( )

def print_stats_table(f, db_stats):
	f.write("<TABLE><TR><TH>Stat name</TH>\n")
	for db in sorted(db_stats.keys()):
		f.write("<TH>%s</TH>" % db)
	f.write("</TH>\n")
	write_row_stat(db_stats, f, "Job Count", "get_total_job_count")
	write_row_stat(db_stats, f, "Failed Job Count", "get_failed_job_count")
	write_row_stat(db_stats, f, "Failed Jobs %", "get_failed_job_percentage")
	write_row_stat(db_stats, f, "Success Job Count", "get_success_job_count")
	write_row_stat(db_stats, f, "Success Job %", "get_success_job_percentage")
	write_row_stat(db_stats, f, "First job submitted at", "get_first_job_submit_time")
	write_row_stat(db_stats, f, "Last job submitted at", "get_last_job_submit_time")
	write_row_stat(db_stats, f, "Longest turnaround (min)", "get_max_turnaround_time")
	write_row_stat(db_stats, f, "Shortest turnaround (min)", "get_min_turnaround_time")
	write_row_stat(db_stats, f, "Mean turnaround (min)", "get_mean_turnaround_time")
	write_row_stat(db_stats, f, "Std turnaround (min)", "get_stddev_turnaround_time")
	f.write("</TABLE>\n")


def time_diff(sub):
	base = datetime.strptime('2009-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
	diff = sub - base
	return diff.days * 24 * 3600 + diff.seconds


def write_plots(db_stats, f, plots_dir):
	"""
	Write out table of plots to provided file handle for each provided db

	:param db_stats: array of DatabaseStats
	:param f: file handle to open index.html file
	:param plots_dir: path of directory to write plots
	:return: nothing
	"""
	for db_name in sorted(db_stats.keys()):
		plot_jobduration_hist(
			db_stats[db_name].allhours, db_name,
			os.path.join(plots_dir, "%s.jobdurationhist.png" % db_name))
		plot_jobduration_percent_hist(
			db_stats[db_name].resultByDuration['failed'],
			db_stats[db_name].resultByDuration['finished'], db_name,
			os.path.join(plots_dir, "%s.jobdurationfailed.png" % db_name))
		for field in sorted(failedFinishedFields.keys()):
			fff = failedFinishedFields[field]
			threshold = fff['threshold']
			if field == 'username':
				threshold = db_stats[db_name].userJobThreshold
			plot_count(
				fff['label'], db_stats[db_name].failedFinished[field], db_name,
				os.path.join(plots_dir, db_name + "." + field + "hist.png"),
				xlabel_rotation=fff['rotate'], threshold=threshold)
			plot_finished_failed(
				fff['label'], db_stats[db_name].failedFinished[field],
				os.path.join(plots_dir, db_name + "." + field + "failed.png"),
				xlabel_rotation=fff['rotate'], threshold=threshold)

	f.write("<TABLE>\n")
	write_row_plot(db_stats, f, "jobdurationhist.png", '500')
	write_row_plot(db_stats, f, "jobdurationfailed.png", '500')

	for field in sorted(failedFinishedFields.keys()):
		fff = failedFinishedFields[field]
		write_row_plot(db_stats, f, field + "hist.png", fff['width'])
		write_row_plot(db_stats, f, field + "failed.png", fff['width'])

	# plot_job_gap(gaps)
	f.write("</TABLE>\n")


def write_row_stat(db_stats, f, stats_name, function_name):
	f.write("<TR><TD>%s</TD>" % stats_name)
	for db in sorted(db_stats.keys()):
		value = getattr(db_stats[db], function_name)()
		if type(value) == str:
			f.write("<TD align='right'>%s</TD>" % (getattr(db_stats[db], function_name)()))
		else:
			f.write("<TD align='right'>%.2f</TD>" % (getattr(db_stats[db], function_name)()))
	f.write("</TR>\n")


def write_row_plot(db_stats, f, suffix, width):
	f.write("<TR>\n")
	for db in sorted(db_stats.keys()):
		f.write("<TD><IMG width=%s src=%s.%s></TD>\n" % (width, db, suffix))
	f.write("</TR>\n")


# Class for printing stats of nsg database

class DatabaseStats:
	def __init__(self, db):
		self.db = db
		if "startdate" in DATABASES[db]:
			self.startdate = DATABASES[db]['startdate']
		else:
			self.startdate = None
		self.userJobThreshold = DATABASES[db]['userJobThreshold']
		self.failedFinished, self.resourcesByResult, self.toolsByResult, self.usersByResult = { }, { }, { }, { }
		self.allhours, self.turnaround_mins, self.submit_times = [], [], []
		self.earliest_ts = datetime.fromtimestamp(time.time())
		self.latest_ts = datetime.strptime('2009-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
		self.resultByDuration = {'finished': [], 'failed': []}

		err_cond1 = "\'%Error submitting job%\'"
		err_cond2 = "\'%java.io.FileNotFoundException%\'"
		err_cond3 = "\'%java.io.IOException%\'"
		err_cond4 = "\'%Authentication failed%\'"
		err_cond5 = "\'%No route to host%\'"
		err_cond6 = "\'%com.trilead.ssh2.SFTPException%\'"
		err_cond7 = "\'%Failed to load results.%\'"
		err_cond8 = "\'%Unable to verify that job%\'"
		err_cond9 = "\'%org.ngbw.sdk.ValidationException%\'"
		err_cond10 = "\'%org.ngbw.sdk.database.StaleRowException%\'"
		err_cond11 = "\'%org.ngbw.sdk.database.NotExistException: No row%\'"
		err_cond12 = "\'%Job working directory already exists%\'"
		err_cond13 = "\'%Too many tasks are waiting to run%\'"
		err_cond14 = "\'%one or more results files are too large%\'"
		err_cond15 = "\'%piseEval error%\'"
		err_cond16 = "\'%java.lang.NullPointerException%\'"
		err_cond17 = "\'%java.lang.IndexOutOfBoundsException%\'"
		# self.elist = [err_cond1, err_cond2, err_cond3, err_cond4, err_cond5, err_cond6, err_cond7, err_cond8, err_cond9, err_cond10, err_cond11, err_cond12, err_cond13, err_cond14, err_cond15, err_cond16, err_cond17]
		self.elist = [err_cond1, err_cond2]

	def computeBursts(self, x, x_dict):
		if (x[0] == 'FAILED'):
			count = 1
		else:
			count = 0
		for i in range(1, len(x)):
			if x[i - 1] == 'FINISHED':
				if x[i] == 'FAILED': count = 1
			if x[i - 1] == 'FAILED':
				if x[i] == 'FAILED':
					count += 1
				else:
					try:
						x_dict[count] += 1
					except:
						x_dict[count] = 1

	def connect(self):
		print "Connecting to '%s'" % (self.db)
		try:
			cnf = os.path.expanduser("~/.my.cnf")
			self.conn = pymysql.connect(read_default_group=self.db, database=DATABASES[self.db], read_default_file=cnf)
			self.cursor = self.conn.cursor()
		except pymysql.Error as err:
			sys.stderr.write("%s\n" % str(err))
			exit(1)

	def disconnect(self):
		self.conn.close()

	def get_first_job_submit_time(self):
		return self.earliest_ts.strftime('%Y-%m-%d %H:%M:%S')

	def get_last_job_submit_time(self):
		return self.latest_ts.strftime('%Y-%m-%d %H:%M:%S')

	def get_failed_job_count(self):
		return len(self.resultByDuration['failed'])

	def get_failed_job_percentage(self):
		return 100 * (float(self.get_failed_job_count()) / self.get_total_job_count())

	def get_max_turnaround_time(self):
		return max(self.turnaround_mins)

	def get_mean_turnaround_time(self):
		return np.mean(self.turnaround_mins)

	def get_min_turnaround_time(self):
		return min(self.turnaround_mins)

	def get_stddev_turnaround_time(self):
		return np.std(self.turnaround_mins)

	def get_success_job_count(self):
		return len(self.resultByDuration['finished'])

	def get_success_job_percentage(self):
		return 100 * (float(self.get_success_job_count()) / self.get_total_job_count())

	def get_total_job_count(self):
		return self.total_jobs

	def plot_bursts(self):
		fields = " date_entered, name, resource"
		condition = (self.condition + " AND (resource='gordon' OR"
									  " resource='trestles') AND (value like %s OR"
									  " value is NULL) order by resource, date_entered")
		query = "SELECT" + fields + self.fromtables + condition

		for each in self.elist:
			g, t = [], []
			self.cursor.execute(query % each)
			for (de, name, res) in self.cursor:
				if res == 'gordon':
					g.append(name)
				elif res == 'trestles':
					t.append(name)
			go, tr = {}, {}

			self.computeBursts(g, go)
			self.computeBursts(t, tr)

			plot_failure_bursts(go, 'gordon', each)
			plot_failure_bursts(tr, 'trestles', each)

	def print_error_counts(self):
		fields = " count(*)"
		condition = self.condition + " AND value like %s AND (resource='gordon' OR resource='trestles')"

		query = "SELECT" + fields + self.fromtables + condition
		for each in self.elist:
			print query % each
			q = self.cursor.execute(query % each)
			for count in self.cursor: print each, count

		query = query + " AND date_entered>'2015-01-01'"
		for each in self.elist:
			print query % each
			q = self.cursor.execute(query % each)
			for count in self.cursor: print each, count

	def read_data(self):
		for field in failedFinishedFields.keys():
			self.failedFinished[field] = { }
		fields = " date_submitted, TIMESTAMPDIFF(hour, date_submitted,date_terminated), TIMESTAMPDIFF(MINUTE, date_submitted,date_terminated), name, " + ", ".join(
			sorted(failedFinishedFields.keys()))
		fromtables = " from job_stats, job_events, users"
		condition = " where job_stats.jobhandle=job_events.jobhandle AND job_stats.user_id = users.user_id AND (job_events.name='FINISHED' OR job_events.name='FAILED')"
		if self.startdate:
			condition += " AND date_submitted > '%s'" % self.startdate
		query = "SELECT" + fields + fromtables + condition
		print query
		self.cursor.execute(query)

		# mycount = 0
		for values in self.cursor:
			(sub, hours, mins, name) = values[0:4]  # grab first 4 values
			result = name.lower()
			if not sub or not name or not mins:
				continue
			for i, field in enumerate(sorted(failedFinishedFields.keys())):
				field_value = values[4 + i]  # get rest of values
				if not (field_value in self.failedFinished[field]):
					self.failedFinished[field][field_value] = { 'finished': 0, 'failed': 0 }
				self.failedFinished[field][field_value][result] += 1
			self.allhours.append(hours)
			self.turnaround_mins.append(mins)
			if sub < self.earliest_ts: self.earliest_ts = sub
			if sub > self.latest_ts: self.latest_ts = sub
			# if user_id == 83567: mycount+=1
			self.resultByDuration[result].append(hours)
		# submit_time.append([self.time_diff(sub),'S'])
		# submit_time.append([self.time_diff(sub),'F'])
		# if user_id == 83567: print "User 83567 (" + str(user_id) + ") has " + str(mycount) + " instances but says " + str(users[user_id])
		# submit_time.sort()
		gaps = []
		# for i in range(len(submit_time)-1):gaps.append(submit_time[i+1][0]-submit_time[i][0])
		# pickle.dump(gaps,open('gaps.pkl','wb'))
		# gaps = pickle.load(open('gaps.pkl','rb'))
		self.total_jobs = \
			len(self.resultByDuration['failed']) + \
			len(self.resultByDuration['finished'])


def main(argv):
	stats_folder_name = "stats-" + datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d')
	stats_path = os.path.join(HTMLDIR, stats_folder_name)
	if not (os.path.exists(stats_path)):
		os.makedirs(stats_path)
	if not DATABASES:
		sys.stderr.write("No databases found, configure stats.conf\n")
		sys.exit(1)
	database_stats = {}
	for db in sorted(DATABASES.keys()):
		database_stats[db] = DatabaseStats(db)
		database_stats[db].connect()
		database_stats[db].read_data()
	f = open(os.path.join(stats_path, "index.html"), "w")
	f.write("<HTML><HEAD>\n")
	if CSS:
		f.write("<link rel=\"stylesheet\" type=\"text/css\" href=\"%s\">\n" % CSS)
	f.write("<TITLE>Stats</TITLE></HEAD><BODY>\n")
	try:
		print_stats_table(f, database_stats)
		write_plots(database_stats, f, stats_path)
	# stats.plot_bursts()
	# stats.print_error_counts()
	finally:
		for db in sorted(DATABASES.keys()):
			database_stats[db].disconnect()
	f.write("</BODY></HTML>\n")
	f.close()


if __name__ == "__main__":
	sys.exit(main(sys.argv))
