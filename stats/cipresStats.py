import pymysql, sys, pickle
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime


#Plotting functions
def plot_jobduration_percent_hist(failed, finished):
	r = max(max(finished),max(failed))
	bins = list(np.arange(0,401,10))
	yfi,xfi = np.histogram(finished,bins=bins)
	yfa,xfa = np.histogram(failed,bins=bins)
	y=yfi+yfa 
	yfi=np.array(yfi,dtype=float)/np.array(y,dtype=float) 
	yfa=np.array(yfa,dtype=float)/np.array(y,dtype=float) 

	plt.ylabel('Fraction of finished/failed jobs',fontsize='15')
	plt.xlabel('Job duration (in hours)',fontsize='15')
	pfi = plt.bar(np.arange(0,len(bins)-1),yfi,color='b',width=1)
	pfa = plt.bar(np.arange(0,len(bins)-1),yfa,bottom=yfi,color='0.75',width=1)
	plt.xticks(np.arange(0,40,4),np.arange(0,400,40))
	plt.legend((pfi[0],pfa[0]),('Finished','Failed'), loc='lower right')
	plt.show()

def plot_resource_finished_failed(gordon, trestles):
	plt.figure()
	plt.ylabel('Fraction of finished/failed jobs',fontsize=15)
	plt.xlabel('Resource',fontsize=15)
	gor, tres = gordon['failed']+gordon['finished'], trestles['failed']+trestles['finished']
	failed = [gordon['failed']/(gor*1.0), trestles['failed']/(tres*1.0)]
	finished = [gordon['finished']/(gor*1.0), trestles['finished']/(tres*1.0)]
	
	w=0.2
	pfi = plt.bar(np.arange(0.2,2*(w+0.1),w+0.1),finished,width=w,color='b')
	pfa = plt.bar(np.arange(0.2,2*(w+0.1),w+0.1),failed,bottom=finished,width=w,color='0.75')
	plt.xticks(np.arange(0.2+w/2,2*(w+0.2),w+0.1),['GORDON','TRESTLES'])
	#plt.legend((pfi[0],pfa[0]),('Finished','Failed'), bbox_to_anchor=[w+0.16, 0.6], loc='lower right')
	plt.legend((pfi[0],pfa[0]),('Finished','Failed'), loc='lower right')
	plt.show()

def plot_tools_finished_failed(tools):
	plt.figure()
	plt.ylabel('Fraction of finished/failed jobs',fontsize=10)
	plt.xlabel('CIPRES tools',fontsize=10)
	ids = tools.keys()
	finished, failed = [], []
	for each in ids:
		finished.append(tools[each]['finished'])
		failed.append(tools[each]['failed'])
	for i in range(len(finished)):
		t = finished[i]+failed[i]+0.0
		finished[i],failed[i]=finished[i]/t,failed[i]/t
	
	pfi = plt.bar(np.arange(0,len(finished)),finished,color='b',width=1)
	pfa = plt.bar(np.arange(0,len(finished)),failed,bottom=finished,color='0.75',width=1)
	plt.xticks(np.arange(len(finished))+0.5,ids,rotation=90,fontsize=8,ha='right')
	plt.xlim([0,len(finished)])
	plt.legend((pfi[0],pfa[0]),('Finished','Failed'), loc='lower right')
	plt.show()

def plot_job_gap(gap):
	plt.figure()
	MIN, MAX = .0000001, 1000000
	bins = 10 ** np.linspace(np.log10(MIN), np.log10(MAX), 50)
	y,x = np.histogram(gap,bins=bins)
	plt.ylabel('Number of jobs')
	plt.xlabel('Time difference from the last job (in hours)')
	pf = plt.bar(np.arange(0,len(bins)-1),y,color='b',width=1)
	plt.show()

def plot_failure_bursts(x_dict,x_name,err):
	plt.figure()
	x = x_dict.keys()
	plt.xlabel('Burst length', fontsize=15)
	plt.ylabel('Number of bursts', fontsize=15)
	plt.title('Failure bursts on ' + x_name + ' due to error type ' + err)
	pf = plt.bar(np.arange(len(x)),x_dict.values(),color='b',width=1)
	plt.xticks(np.arange(len(x))+0.5,x,rotation=0,fontsize=7)
	plt.savefig('bursts/'+x_name+err+'.png')
	plt.show()

#Class for printing stats of cipres database

class cipresStats:
	def __init__(self):
		self.fields = " date_submitted, TIMESTAMPDIFF(hour, date_submitted,date_terminated), name, resource, tool_id"
		self.fromtables = " from job_stats, job_events"
		self.condition = " where job_stats.jobhandle=job_events.jobhandle AND (job_events.name='FINISHED' OR job_events.name='FAILED')"

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
		self.elist = [err_cond1, err_cond2, err_cond3, err_cond4, err_cond5, err_cond6, err_cond7, err_cond8, err_cond9, err_cond10, err_cond11, err_cond12, err_cond13, err_cond14, err_cond15, err_cond16, err_cond17]

	def connect(self):
		cpwd = 'cipresRO'
		try:
			self.conn = pymysql.connect(host='mysql2.sdsc.edu',port=3312,user='readonly',passwd=cpwd,db='cipres')
			self.cursor = self.conn.cursor()
		except pymysql.Error as err:
			sys.stderr.write("%s\n" % str(err))
			exit(1)

	def disconnect(self):
		self.conn.close()

	def time_diff(self, sub):
		base = datetime.strptime('2009-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
		diff = sub - base
		return diff.days*24*3600+diff.seconds

	def plotHistograms(self):
		query = "SELECT" + self.fields + self.fromtables + self.condition
		self.cursor.execute(query)

		failed, finished, submit_time = [], [], []
		gordon, trestles = {'finished':0, 'failed':0}, {'finished':0, 'failed':0}
		tools = {}
		
		for (sub,days,name,resource,tool_id) in self.cursor:
			if name=='FINISHED':
				if days:finished.append(days)
				if sub:submit_time.append([self.time_diff(sub),'S'])
				if resource=='gordon':gordon['finished']+=1
				if resource=='trestles':trestles['finished']+=1
				try:tools[tool_id]['finished']+=1
				except:
					tools[tool_id]={}
					tools[tool_id]['finished']=1
					tools[tool_id]['failed']=0
			if name=='FAILED':
				if days:failed.append(days)
				if sub:submit_time.append([self.time_diff(sub),'F'])
				if resource=='gordon':gordon['failed']+=1
				if resource=='trestles':trestles['failed']+=1
				try:tools[tool_id]['failed']+=1
				except:
					tools[tool_id]={}
					tools[tool_id]['failed']=1
					tools[tool_id]['finished']=0	

		submit_time.sort()
		gaps = []
		for i in range(len(submit_time)-1):gaps.append(submit_time[i+1][0]-submit_time[i][0])
		#pickle.dump(gaps,open('gaps.pkl','wb'))
		#gaps = pickle.load(open('gaps.pkl','rb'))
		
		plot_jobduration_percent_hist(failed, finished)		
		plot_resource_finished_failed(gordon, trestles)
		plot_tools_finished_failed(tools)
		plot_job_gap(gaps)

	def computeBursts(self, x, x_dict):	
		if(x[0]=='FAILED'):count=1
		else:count=0
		for i in range(1,len(x)):
			if x[i-1]=='FINISHED':
				if x[i]=='FAILED':count=1
			if x[i-1]=='FAILED':
				if x[i]=='FAILED':count += 1
				else:
					try:x_dict[count]+=1
					except:x_dict[count]=1

	def plotBursts(self):
		fields = " date_entered, name, resource"
		condition = (self.condition + " AND (resource='gordon' OR" 
		" resource='trestles') AND (value like %s OR"
		" value is NULL) order by resource, date_entered") 
		query = "SELECT" + fields + self.fromtables + condition

		for each in self.elist:
			g, t = [], []
			self.cursor.execute(query%each)
			for (de, name, res) in self.cursor:
				if(res=='gordon'):g.append(name)
				elif(res=='trestles'):t.append(name)
			go,tr={},{}

			self.computeBursts(g,go)
			self.computeBursts(t,tr)

			plot_failure_bursts(go, 'gordon', each)	
			plot_failure_bursts(tr, 'trestles', each)

	def printErrorCounts(self):
		fields = " count(*)"
		condition = self.condition + " AND value like %s AND (resource='gordon' OR resource='trestles')"
 
		query = "SELECT" + fields + self.fromtables + condition	
		for each in self.elist:
			q = self.cursor.execute(query%each)
			for count in self.cursor: print each, count

		query = query + " AND date_entered>'2015-01-01'"
		for each in self.elist:
			q = self.cursor.execute(query%each)
			for count in self.cursor: print each, count


def main(argv):
	stats = cipresStats()
	try:
		stats.connect()
		stats.plotHistograms()
		stats.plotBursts()
		stats.printErrorCounts()
	finally:
		stats.disconnect()

if __name__ == "__main__":
    sys.exit(main(sys.argv))
