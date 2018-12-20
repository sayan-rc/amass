from feature import feature
import pymysql, sys, pickle, pytz
import numpy as np
from dateutil.parser import parse
from datetime import datetime
from bisect import *
from copy import deepcopy
import os, re, shutil

class getSubmitFeatures:
    def __init__(self, config ):
        self.config = config
        self.localdb = config.get("amassdb", "database")
        self.localpass = config.get("amassdb", "password")
        self.localuser = config.get("amassdb", "user")
        self.datadir = config.get("amass", "datadir")
        self.base = datetime.strptime('2009-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
        self.testbase = parse('2008-01-01T00:00:00.000-08:00')
        sys.stdout.write("Loading data...")
        sys.stdout.flush()
        self.tests = pickle.load(open(os.path.join(self.datadir,'tests.pkl'),'rb'))
        self.tools = pickle.load(open(os.path.join(self.datadir,'tool_ids.pkl'),'rb'))
        print "done"
        self.resources=re.split("\s*,\s*", self.config.get("resources", "db"))
        self.gw_resources=re.split("\s*,\s*", self.config.get("resources", "gateway"))
        self.resource_no={}
        for i, resource in enumerate(self.resources):
            self.resource_no[resource] = i
	self.limit=1
	self.test_names=self.tests.keys()	

    def Connect(self):
        #connect to local db
        db_attrs = {}
        for name, value in self.config.items("amassdb"):
           if name == 'port':
                db_attrs[name] = int(value)
           else:
                db_attrs[name] = value

        try:
            self.localConn=pymysql.connect(**db_attrs)
        except pymysql.Error as err:
            sys.stderr.write("%s\n" % str(err))

    def Disconnect(self):
        #disconnect from local db
        self.localConn.close()

    def sortResourceJobs(self, resource, cursor):
        #sort jobs before searching for the last job
        jobs = []
	cursor.execute("SELECT date_entered, date_terminated, name from event_stats where resource=%s",resource)
        for (de, dt, name) in cursor:
            diff_entered= de - self.base
            e = diff_entered.days*24*3600+diff_entered.seconds
            if dt:
                diff_terminated= dt - self.base
            else: 
                diff_terminated= diff_entered
            t = diff_terminated.days*24*3600+diff_terminated.seconds
            jobs.append([e,t,name])
	jobs.sort()
        return jobs

    def sortToolJobs(self, tool, cursor):
	jobs=[]
	cursor.execute("SELECT date_entered, date_terminated, name from event_stats where tool_id=%s",tool)
        for (de, dt, name) in cursor:
            diff_entered= de - self.base
            e = diff_entered.days*24*3600+diff_entered.seconds
            if dt:
                diff_terminated= dt - self.base
            else:
                diff_terminated= diff_entered
            t = diff_terminated.days*24*3600+diff_terminated.seconds
            jobs.append([e,t,name])
	#print len(jobs)
        jobs.sort()
	return jobs	

    def _testFeatures(self, date_entered):
        #return test features before date_start
        diff = date_entered.replace(tzinfo=pytz.timezone('America/Los_Angeles')) - self.testbase
        st = diff.days*24*3600+diff.seconds 
        feat = []
        feat_time = []
        for each in sorted(self.test_names):
            i = bisect_left(self.tests[each], [st,0, ])
            if(i): i = i-1
            feat.append(self.tests[each][i][1])
            print "(%s,%i) - Setting feature %s to %s" % (str(date_entered), i, each, self.tests[each][i][1])
	    #print self.tests[each][i][0]
            #feat_time.append(st-self.tests[each][i][0])
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
	if(j): j=j-1
	count=0
        lastJobs, lastToolJob, jobs_since, submitted_since=[],[],[],[]
	jobs_past=0
	subs=0
	#get burst length
	p=i
	while x[p][2]!='FINISHED':
	    if x[p][1]<=e:
	    	jobs_past+=1
	    p-=1
        jobs_since.append(jobs_past)
	while(i>=0 and count<self.limit):
            if(x[i][1]<=e):
            	submitted_since.append(subs)
		lastJobs.append(x[i][2]=='FINISHED')
		count+=1
		subs=0
            i = i - 1
	    subs+=1
        while count<self.limit:
	    submitted_since.append(subs)
            lastJobs.append(True)
            count+=1
	    subs=0
	jobs_past=0
	p=j
	while tx[p][2]!='FINISHED':
	    if x[p][1]<=e:
            	jobs_past+=1
            p-=1
	jobs_since.append(jobs_past)
	subs=0
	while(j>=0):
	    if tx[j][1]<=e:
		submitted_since.append(subs)
	    	lastToolJob.append(tx[j][2]=='FINISHED')
		break
	    j=j-1
	    subs+=1
	if not lastToolJob: 
	    lastToolJob.append(1==1)
	    submitted_since.append(0)
        return lastJobs, lastToolJob, jobs_since, submitted_since

    def generateTrainingData(self,err,localCursor,query,sorted_resource_jobs,sorted_tool_jobs):
        print query % err
        localCursor.execute(query,(err))
        X,Y,D=[],[],[]
        count = 0
	ts=sorted((self.tools).items(), key=lambda x: x[1])
	tool_names=[]
	for each in ts: tool_names.append(each[0])
        feat_names =(['resource']*len(self.resources))+tool_names+self.tests.keys()+['submitted_since']*(self.limit+1)+(['jobs_since']*(self.limit+1))+['last_tool_job']+(['last_job']*self.limit)
	print 'tools '+str(len(self.tools.keys())), 'tests '+str(len(self.tests.keys()))
	for (jid,res, tool, date_entered, name) in localCursor:
            count += 1
            res_feat=[0]*len(self.resources)
            res_feat[self.resource_no[res]]=1
            test_feat = self._testFeatures(date_entered)
            tool_feat = [0]*(len(self.tools.keys()))
            tool_feat[self.tools[tool]] = 1
            lastJobs, lastToolJob, jobs_since_feat, submitted_since = self._lastJobResult(sorted_resource_jobs,res, sorted_tool_jobs, self.tools[tool],date_entered)

            feat = feature(jid,res_feat, tool_feat, test_feat, jobs_since_feat, lastJobs, test_time=submitted_since, otherFeatures=lastToolJob)
            X.append(feat.toList())
            Y.append(int(name=='FINISHED'))
            D.append(date_entered)
            if(count%1000==0):
                sys.stdout.write("Created %d feature vectors.\n" % count)
        sys.stdout.write("Created %d feature vectors.\n" % count)
        XPath=os.path.join(self.datadir,err+'X')
        YPath=os.path.join(self.datadir,err+'Y')
        DPath=os.path.join(self.datadir,err+'D')
        if os.path.isdir(XPath): shutil.rmtree(XPath)
        if os.path.isdir(YPath): shutil.rmtree(YPath)
        if os.path.isdir(DPath): shutil.rmtree(DPath)
	if len(Y)>2:
            print "Dumping features of type %s"%err
            #create a new folder
            #define a beginning point and put overflow in files
            os.mkdir(XPath)
            os.mkdir(YPath)
            os.mkdir(DPath)
            beg=0
            count=1
            while len(Y)>beg:
                end = min(50000*count, len(Y))
                print os.path.join(XPath,str(count)+'.pkl')
                pickle.dump(X[beg:end],open(os.path.join(XPath,str(count)+'.pkl'),'wb'))
                pickle.dump(Y[beg:end],open(os.path.join(YPath,str(count)+'.pkl'),'wb'))
                print os.path.join(DPath,str(count)+'.pkl')
                pickle.dump(D[beg:end],open(os.path.join(DPath,str(count)+'.pkl'),'wb'))
                beg=end
                count+=1
        pickle.dump(feat_names,open(os.path.join(self.datadir,'feat_names.pkl'),'wb'))

    def buildJobFeatures(self,fname,startDate,endDate):
        print "Building job features for %s" % fname
        localCursor = self.localConn.cursor()
        try:
            #use the given database
            localCursor.execute("USE " + self.localdb)

            #get sorted jobs from resources
            sorted_resource_jobs={}
            for i, r in enumerate(self.resources):
                sorted_resource_jobs[r]=self.sortResourceJobs(self.gw_resources[i], localCursor)

	    sorted_tool_jobs = {}
	    for t in self.tools.keys():
		sorted_tool_jobs[self.tools[t]]=self.sortToolJobs(t,localCursor)
            #query event_stats table 
            fields = ("remote_job_id,resource, tool_id, date_entered,name")
            where = "where date_entered>"+startDate+" AND date_entered<" + endDate +" AND (value like %s)"
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

	    query2 = "SELECT local_jobid FROM gateway_xsede where date_entered>"+startDate+" AND date_entered<" + endDate
	    localCursor.execute(query2)
	    xsede_ids=set([])
	    for (lid,) in localCursor: xsede_ids.add(lid)
	    pickle.dump(xsede_ids, open(os.path.join(self.datadir,'xsedeids.pkl'),'wb'))
	        
        finally:
            #close the connection
            localCursor.close()
