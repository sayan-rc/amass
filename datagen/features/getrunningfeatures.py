from feature import feature
import pymysql, sys, pickle, pytz
import numpy as np
from dateutil.parser import parse
from datetime import datetime
from bisect import *
from copy import deepcopy
import os, shutil

class getRunningFeatures:
    def __init__(self, localdb, localpass, localuser, datadir, runonlima):
        self.localdb = localdb
        self.localpass = localpass
        self.localuser = localuser
        self.datadir = datadir
        self.base = datetime.strptime('2009-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
        self.testbase = parse('2008-01-01T00:00:00.000-08:00')
        self.tests = pickle.load(open(datadir+'tests.pkl','rb'))
        self.tools = pickle.load(open(datadir+'tool_ids.pkl','rb'))
        self.runonlima = runonlima
        self.resources=[]
        self.resource_no={}
	self.limit=1
    	for l in open(datadir+'resources.txt'):
            x=l.strip()
            if x:
                x=x.split()
                self.resources.append((x[0],int(x[1])))

        i=0
        for r in self.resources:
            self.resource_no[r[0].lower()]=i
            i+=1

    def Connect(self):
        #connect to local db
        try:
            if self.runonlima:
                self.localConn=pymysql.connect(user=self.localuser)
            else:
                self.localConn=pymysql.connect(user=self.localuser,passwd=self.localpass)
        except pymysql.Error as err:
            sys.stderr.write("%s\n" % str(err))

    def Disconnect(self):
        #disconnect from local db
        self.localConn.close()

    def sortResourceJobs(self, resource, cursor):
        #sort jobs before searching for the last job
        jobs = []
        
	cursor.execute("SELECT date_submitted, date_terminated, name from event_stats where resource=%s and date_submitted is not null",resource)
        for (ds, dt, name) in cursor:
            if not ds:
                continue
            diff_submitted= ds - self.base
            s = diff_submitted.days*24*3600+diff_submitted.seconds
            if dt:
                diff_terminated= dt - self.base
            else: 
                diff_terminated= diff_submitted
            t = diff_terminated.days*24*3600+diff_terminated.seconds
            jobs.append([s,t,name])
	print 'Sorting resource jobs..'
	jobs.sort()
        return jobs

    def sortToolJobs(self, tool, cursor):
        jobs=[]
	cursor.execute("SELECT date_submitted, date_terminated, name from event_stats where tool_id=%s and date_submitted is not null",tool)
        for (ds, dt, name) in cursor:
            if not ds:
                continue
            diff_submitted= ds - self.base
            s = diff_submitted.days*24*3600+diff_submitted.seconds
            if dt:
                diff_terminated= dt - self.base
            else:
                diff_terminated= diff_submitted
            t = diff_terminated.days*24*3600+diff_terminated.seconds
            jobs.append([s,t,name])
	    #print len(jobs)
	print 'Sorting tool jobs'
        jobs.sort()
        return jobs

    def _simultaneousJobFeature(self, res, date_start, sorted_resource_jobs):
        diff_started = date_start - self.base
        s = diff_started.days*24*3600+diff_started.seconds
    
        #find the jobs that are running at the time this job starts running
        x=sorted_resource_jobs[res]
        #if res=='gordon': x=self.gordon
        #if res=='trestles': x=self.trestles
        i = bisect_left(x, [s,])
        if(i):i-=1
	concur_jobs=[]
        while(i>=0):
            if(x[i][1]>s): concur_jobs.append(x[i])
            i-=1

        job_feat=[]
        #get all the attributes of the overlapping jobs
        job_feat.append(len(concur_jobs))
        wd, nc, proc= [], [], []
        for p in concur_jobs:
            wd.append(s-p[0])  
            nc.append(p[3])  
            proc.append(p[4])    
	
        if wd: 
            job_feat.extend([max(wd), np.mean(wd)])
        else:
            job_feat.extend([0,0])
	if nc: 
            job_feat.extend([max(nc), np.mean(nc)])
        else:
            job_feat.extend([0,0])
        if proc: 
            job_feat.extend([max(proc), np.mean(proc)])
        else:
            job_feat.extend([0,0])
    	
	return job_feat

    def _testFeatures(self, date_submitted):
        #return test features before date_start
        diff = date_submitted.replace(tzinfo=pytz.timezone('America/Los_Angeles')) - self.testbase
        st = diff.days*24*3600+diff.seconds 
        feat = []
        feat_time = []
        for each in self.tests.keys():
            i = bisect_left(self.tests[each], [st,0, ])
            if(i): i = i-1
            feat.append(self.tests[each][i][1])
	    #print self.tests[each][i][0]
            #feat_time.append(np.log(self.tests[each][i][0]+1))
        return feat

    def _lastJobResult(self, sorted_resource_jobs, res, sorted_tool_jobs, tool, date_entered):
        #get the result of the last job for job which started at date_entered
        x=sorted_resource_jobs[res]
    	tx=sorted_tool_jobs[tool]
        #if(res=='gordon'): x=gordon
        #elif(res=='trestles'): x=trestles
        diff = date_entered - self.base
        e = diff.days*24*3600+diff.seconds
        i = bisect_left(x, [e,])
        j = bisect_left(tx,[e,])
	if(i):i=i-1
	if(j):j=j-1
    	count=0
        lastJobs, lastToolJob, jobs_since=[],[],[]
    	jobs_past=0
        while(i>=0 and count<self.limit):
            if(x[i][1]<=e):
                #jobs_since.append(jobs_past)
        	lastJobs.append(x[i][2]=='FINISHED')
        	count+=1
        	jobs_past=0
            i = i - 1
            jobs_past+=1
        while count<self.limit:
            #jobs_since.append(jobs_past)
            lastJobs.append(True)
            count+=1
            jobs_past=0
    	jobs_past=0
    	while(j>=0):
            if tx[j][1]<=e:
            	lastToolJob.append(tx[j][2]=='FINISHED')
        	#jobs_since.append(jobs_past)
        	break
            j=j-1
            jobs_past+=1
    	if not lastToolJob: 
            lastToolJob.append(1==1)
            #jobs_since.append(jobs_past)
        return lastJobs, lastToolJob, jobs_since

    def generateTrainingData(self,err,localCursor,query,sorted_resource_jobs,sorted_tool_jobs):
        print query % err
        localCursor.execute(query,(err))
        X,Y,D=[],[],[]
        count = 0
	for (jid,res, tool, date_submitted, name) in localCursor:
            if not date_submitted: continue
	    count += 1
            #diff_date=date_entered - self.base
            #e=diff_date.days*24*3600+diff_date.seconds
            res_feat=[0]*len(self.resources)
            res_feat[self.resource_no[res]]=1
            test_feat = self._testFeatures(date_submitted)
            tool_feat = [0]*(len(self.tools.keys()))
            tool_feat[self.tools[tool]] = 1
            #q_feat=[int(queue=='shared')]
	    #nc=[nodecount]
	    #prcs=[processors]
            lastJobs, lastToolJob, jobs_since_feat = self._lastJobResult(sorted_resource_jobs,res, sorted_tool_jobs, self.tools[tool],date_submitted)
            #sjf = self._simultaneousJobFeature(res, date_submitted, sorted_resource_jobs)
            
            feat = feature(jid,res_feat, tool_feat, test_feat,[], lastJobs)
            X.append(feat.toList())
            Y.append(int(name=='FINISHED'))
            D.append(date_submitted)
            if(count%1000==0):
                sys.stdout.write("Created %d feature vectors.\n" % count)
        feat_names =(['resource']*len(self.resources))+self.tools.keys()+self.tests.keys()+(['last_job']*self.limit)
	sys.stdout.write("Created %d feature vectors.\n" % count)
        XPath=self.datadir+err+'X'
        YPath=self.datadir+err+'Y'
        DPath=self.datadir+err+'D'
	if os.path.isdir(XPath): shutil.rmtree(XPath)
        if os.path.isdir(YPath): shutil.rmtree(YPath)
        if os.path.isdir(DPath): shutil.rmtree(DPath)
	if len(Y)>0:
            print "Dumping features of type %s"%err
            #create a new folder
            #define a beginning point and put overflow in files
            os.mkdir(self.datadir+err+'X')
            os.mkdir(self.datadir+err+'Y')
            os.mkdir(self.datadir+err+'D')
            beg=0
            count=1
            while len(Y)>beg:
                end = min(50000*count, len(Y))
                pickle.dump(X[beg:end],open(self.datadir+err+'X'+'/'+str(count)+'.pkl','wb'))
                pickle.dump(Y[beg:end],open(self.datadir+err+'Y'+'/'+str(count)+'.pkl','wb'))
                pickle.dump(D[beg:end],open(self.datadir+err+'D'+'/'+str(count)+'.pkl','wb'))
                beg=end
                count+=1
        pickle.dump(feat_names,open(self.datadir+'feat_names.pkl','wb'))

    def buildJobFeatures(self,fname,startDate,endDate):
        localCursor = self.localConn.cursor()
        try:
            #use the given database
            localCursor.execute("USE " + self.localdb)

            #get sorted jobs from resources
            sorted_resource_jobs={}
            for r in self.resources:
                sorted_resource_jobs[r[0].lower()]=self.sortResourceJobs(r[0].lower(), localCursor)

	    sorted_tool_jobs = {}
	    for t in self.tools.keys():
		sorted_tool_jobs[self.tools[t]]=self.sortToolJobs(t,localCursor)
            #query event_stats table 
            fields = ("remote_job_id,resource, tool_id,date_submitted, name")
            where = "where date_entered>"+startDate+" AND date_entered<" + endDate +"AND date_submitted is not null AND (value like %s)"
            err_cond1 = "\'%Error submitting job%\'"
            err_cond2 = "\'%java.io.FileNotFoundException%\'"
            err_cond3 = "\'%java.io.IOException%\'"
            err_cond4 = "\'%Authentication failed%\'"
            err_cond5 = "\'%No route to host%\'"
            err_cond6 = "\'%com.trilead.ssh2.SFTPException%\'"
            err_cond7 = "\'%Failed to load results.%\'"
            err_cond8 = "\'%Unable to verify that job%\'"
            ucond1 = "\'%piseEval error%\'"
            ucond2 = "\'%org.ngbw.sdk.ValidationException%\'"
            pcond1 = "\'%org.ngbw.sdk.database.StaleRowException%\'"
            pcond2 = "\'%org.ngbw.sdk.database.NotExistException%\'"
            pcond3 = "\'%Job working directory already exists%\'"
            pcond4 = "\'%java.lang.NullPointerException%\'"
            pcond5 = "\'%java.lang.IndexOutOfBoundsException%\'"
            query = "SELECT " + fields + " FROM event_stats " + where
            e = [err_cond1, err_cond2, err_cond3, err_cond4, err_cond5, err_cond6, err_cond7, err_cond8, ucond1, ucond2, pcond1, pcond2, pcond3, pcond4, pcond5, 'NULL']
            #localCursor.execute(query_five%(pcond1,pcond2,pcond3,pcond4,pcond5))
            #localCursor.execute(query_two%(err_cond1,err_cond2))

            #generate features
            for each in e: self.generateTrainingData(each,localCursor,query,sorted_resource_jobs,sorted_tool_jobs)
        finally:
            #close the connection
            localCursor.close()
