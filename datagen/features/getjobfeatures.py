from feature import feature
import pymysql, sys, pickle, pytz
import numpy as np
from dateutil.parser import parse
from datetime import datetime
from bisect import *
from copy import deepcopy
import os, shutil

class getJobFeatures:
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
        self.resources = self._getResources()
	self.errors = self._populateErrorTypes()

    def _populateErrorTypes(self):
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
	return [err_cond1, err_cond2, err_cond3, err_cond4, err_cond5, err_cond6, err_cond7, err_cond8, ucond1, ucond2, pcond1, pcond2, pcond3, pcond4, pcond5, 'NULL']

    def _getResources(self):
	res=[]
	for l in open(self.datadir+'resources.txt'):
                x=l.strip()
                if x:
                    x=x.split()
                    res.append((x[0],int(x[1])))
	return res

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

    def sortXDCDBJobs(self):
        localCursor = self.localConn.cursor()
        try:
            #use the given database
            localCursor.execute("USE " + self.localdb)
            
            #query xsede table
            self.resource_tables={}
            self.resource_no={}
            i=0
            for r in self.resources:
                self.resource_tables[r[0].lower()]=[]
                self.resource_no[r[0].lower()]=i
                i+=1
	    
	    r_id_to_name={}
            for r in self.resources:
                r_id_to_name[r[1]]=r[0].lower()
            #self.gordon, self.trestles = [], []
            query = "SELECT start_time,end_time,wallduration,nodecount,processors,queue,resource_id FROM xsede"
            localCursor.execute(query)

            #add [start, end] intervals to the respective resource lists
            for (stime,etime,wd,nc,procs,queue,rid) in localCursor:
                st = stime - self.base          
		et = etime - self.base
                self.resource_tables[r_id_to_name[rid]].append([st.days*24*3600+st.seconds,et.days*24*3600+et.seconds,wd,nc,procs,queue])
            for r in self.resource_tables.keys():
                self.resource_tables[r].sort()
        finally:
            #close the connection
            localCursor.close()

    #TODO:change features to test - INCA
    def _testFeatures(self, date_start):
        #return test features before date_start
        diff = date_start.replace(tzinfo=pytz.timezone('America/Los_Angeles')) - parse('2008-01-01T00:00:00.000-08:00')
        st = diff.days*24*3600+diff.seconds 
        feat = np.zeros((1,0))
        for each in self.tests.keys():
            i = bisect_left(self.tests[each], [st,0, ])
            if(i): i = i-1
            '''
            if(self.tests[each][i][2].shape[0]>0):
                temp  = deepcopy(self.tests[each][i][2]).toarray()
                temp = np.append(temp, self.tests[each][i][1])
                feat = np.append(feat, temp)
            else:
                feat = np.append(feat, self.tests[each][i][1])
            '''
            feat = np.append(feat, self.tests[each][i][1])
        return feat

    #Commented part of the code not in use. Uncomment to build features with XSEDE
    
    def _singleJobFeature(self, res, tool, date_start, date_term, wd, nc, procs, queue, name):
        diff_entered = date_start - self.base
        s = diff_entered.days*24*3600+diff_entered.seconds
	if date_term==None:
	    t=s+1
	else:
	    diff_terminated = date_term - self.base
            t = diff_terminated.days*24*3600+diff_terminated.seconds

        #find the overlapping jobs
        x=self.resource_tables[res]
        #if res=='gordon': x=self.gordon
        #if res=='trestles': x=self.trestles
        i1 = bisect_left(x, [s,])
        i2 = bisect_right(x, [t,])
        while(i1>=0):
            if(x[i1][1]<s):break
            else:i1 = i1 - 1
        i1 = i1 + 1

        #get all the attributes of the overlapping jobs
        l, wd, nc, proc, qu = [], [], [], [], []
        l.append([s,'S'])
        l.append([t,'E'])
        for i in range(i1, i2):
            l.append([x[i][0],'S'])
            l.append([x[i][1],'E'])
            wd.append(x[i][2])  
            nc.append(x[i][3])  
            proc.append(x[i][4])    
            qu.append(x[i][5])
        l.sort()

        #find maximum overlapping intervals
        count, maxCount = 0, 0 
        for i in range(len(l)):
            if(l[i][1]=='S'):count = count + 1 
            else:count = count - 1 
            if(count>maxCount):maxCount = count

        allConc = len(l)/2-1    
        maxConc = maxCount-1
        duration = (t-s)/(60.0*60.0)
        allConc = allConc/(t-s)
        job_feat = [allConc, maxConc, duration]
        if wd: 
	    job_feat.extend([max(wd), min(wd), np.mean(wd)])
	else:
	    job_feat.extend([0,0,0])
	if nc: 
            job_feat.extend([max(nc), min(nc), np.mean(nc)])
        else:
            job_feat.extend([0,0,0])
	if proc: 
            job_feat.extend([max(proc), min(proc), np.mean(proc)])
        else:
            job_feat.extend([0,0,0])
	return job_feat

    def buildJobFeaturesWithXsede(self,fname,startDate,endDate):
        localCursor = self.localConn.cursor()
        try:
            #use the given database
            localCursor.execute("USE " + self.localdb)
            
	    #get sorted jobs from resources
            sorted_resource_jobs={}
            for r in self.resource_tables.keys():
                sorted_resource_jobs[r]=self._sortResourceJobs(r, localCursor)

            #query event_stats table 
            fields = ("resource,tool_id,date_entered, date_terminated,wallduration,"
            "nodecount,processors,queue,name")
            where = "where date_entered>"+startDate+" AND date_entered<" + endDate + " AND (value like %s)"
	    query = "SELECT " + fields + " FROM cipres_xsede " + where
	    
	    for err in self.errors:
		print query % err
            	localCursor.execute(query,(err))
		
		X,Y,D=[],[],[]
            	
		#generate features
            	count = 0
            	for (res, tool, date_start, date_term, wd, nc, procs, queue, name) in localCursor:
		    count += 1
               	    feat_resources=[0]*len(self.resources)
               	    feat_resources[self.resource_no[res]]=1
                    feat_single = self._singleJobFeature(res, tool, date_start, date_term, wd, nc, procs, queue, name)
                    feat_other = [wd,nc,procs,int(queue=='shared')]
                    test_feat = self._testFeatures(date_start)
                    feat_tools = [0]*len(self.tools.keys())
                    feat_tools[self.tools[tool]] = 1
		    lastJob = self._lastJobResult(sorted_resource_jobs,res, date_start)
		    feat = feature(feat_resources, feat_tools, test_feat, lastJob, feat_single, feat_other)
		    if count==1:
            	    	feat.printFeat()
		    X.append(feat.toList())
            	    Y.append(int(name=='FINISHED'))
            	    D.append(date_start)
                    if(count%10000==0): 
                    	sys.stdout.write("Created %d feature vectors.\n" % count)
		sys.stdout.write("Created %d feature vectors.\n" % count)
            feat_names =(['resource']*len(self.resources))+self.tools.keys()+self.tests.keys()+['last_job']+(['Single_job_feature']*len(feat_single))+['Wallduration']+['node_count']+['processors']+['queue']
	    if len(Y)>2:
            	print "Dumping features of type %s"%err
            	#create a new folder
            	#define a beginning point and put overflow in files
            	XPath=self.datadir+err+'X'
            	YPath=self.datadir+err+'Y'
            	DPath=self.datadir+err+'D'
            	if os.path.isdir(XPath): shutil.rmtree(XPath)
            	os.mkdir(self.datadir+err+'X')
            	if os.path.isdir(YPath): shutil.rmtree(YPath)
            	os.mkdir(self.datadir+err+'Y')
            	if os.path.isdir(DPath): shutil.rmtree(DPath)
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
        finally:
            #close the connection
            localCursor.close()
    

    def _sortResourceJobs(self, resource, cursor):
        #sort jobs before searching for the last job
        jobs = []
        cursor.execute("SELECT date_entered, date_terminated, name from cipres_xsede where resource=%s",resource)
        for (de, dt, name) in cursor:
            diff_entered= de - self.base
            e = diff_entered.days*24*3600+diff_entered.seconds
            if dt:
                diff_terminated= dt - self.base
                t = diff_terminated.days*24*3600+diff_terminated.seconds
            else: t=e
            jobs.append([e,t,name])
        jobs.sort()
        return jobs

    def _lastJobResult(self, sorted_resource_jobs, res, date_entered):
        #get the result of the last job for job which started at date_entered
        x=sorted_resource_jobs[res]
        #if(res=='gordon'): x=gordon
        #elif(res=='trestles'): x=trestles
        diff_entered = date_entered - self.base
        e = diff_entered.days*24*3600+diff_entered.seconds
        i = bisect_left(x, [e,])
        while(i>=0):
            if(x[i][1]<e):break
            else:i = i - 1
        return x[i][2]=='FINISHED'

    def _generateTrainingData(self,err,localCursor,query,sorted_resource_jobs):
        print query % err
        localCursor.execute(query,(err))
        X,Y,D=[],[],[]
        count = 0
        feat_names =(['resource']*len(self.resources))+self.tools.keys()+self.tests.keys()+['last_job']
        for (res, tool, date_entered, name) in localCursor:
            count += 1
	    #diff_date=date_entered - self.base
	    #e=diff_date.days*24*3600+diff_date.seconds
            res_feat=[0]*len(self.resources)
            res_feat[self.resource_no[res]]=1
            test_feat = self._testFeatures(date_entered)
            tool_feat = [0]*(len(self.tools.keys()))
            tool_feat[self.tools[tool]] = 1
            lastJob = self._lastJobResult(sorted_resource_jobs,res, date_entered)

	    feat = feature(res_feat, tool_feat, test_feat, lastJob)
            X.append(feat.toList())
            Y.append(int(name=='FINISHED'))
	    D.append(date_entered)
            if(count%1000==0): 
                sys.stdout.write("Created %d feature vectors.\n" % count)
        sys.stdout.write("Created %d feature vectors.\n" % count)
        if len(Y)>2:
            print "Dumping features of type %s"%err
	    #create a new folder
	    #define a beginning point and put overflow in files
	    XPath=self.datadir+err+'X'
	    YPath=self.datadir+err+'Y'
	    DPath=self.datadir+err+'D'
	    if os.path.isdir(XPath): shutil.rmtree(XPath)
	    os.mkdir(self.datadir+err+'X')
	    if os.path.isdir(YPath): shutil.rmtree(YPath)
	    os.mkdir(self.datadir+err+'Y')
	    if os.path.isdir(DPath): shutil.rmtree(DPath)
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

    def buildJobFeaturesWithoutXSEDE(self,fname,startDate,endDate):
        localCursor = self.localConn.cursor()
        try:
            #use the given database
            localCursor.execute("USE " + self.localdb)

            #get sorted jobs from resources
            sorted_resource_jobs={}
            for r in self.resource_tables.keys():
                sorted_resource_jobs[r]=self._sortResourceJobs(r, localCursor)

            #query event_stats table 
            fields = ("resource,tool_id,date_entered,name")
	    fields1 = ("resource,tool_id,start_time,name")
            where = "where date_entered>"+startDate+" AND date_entered<" + endDate + " AND (value like %s)"
            where_all = "where date_entered>"+startDate+" AND date_entered<" + endDate + " AND (value='NULL' or value regexp %s)"
            where_two = "where date_entered>"+startDate+" AND date_entered<" + endDate + " AND (value='NULL' or value like %s or value like %s)"    
            where_five = "where date_entered>"+startDate+" AND date_entered<" + endDate + " AND (value='NULL' or value like %s or value like %s or value like %s or value like %s or value like %s)"
            sys_err = ("\'Error submitting job|java.io.FileNotFoundException"
            "|java.io.IOException|Authentication failed|No route to host|com.trilead.ssh2"
            ".SFTPException|Failed to load results|Unable to verify that job\'")
            query = "SELECT " + fields + " FROM event_stats " + where
	    query_new = "SELECT " + fields + " FROM cipres_xsede " + where
            query_all = "SELECT " + fields + " FROM event_stats " + where_all
            query_two = "SELECT " + fields + " FROM event_stats " + where_two
            query_five = "SELECT " + fields + " FROM event_stats " + where_five
            #localCursor.execute(query_five%(pcond1,pcond2,pcond3,pcond4,pcond5))
            #localCursor.execute(query_two%(err_cond1,err_cond2))

            #generate features
            for each in self.errors: self._generateTrainingData(each,localCursor,query_new,sorted_resource_jobs)

        finally:
            #close the connection
            localCursor.close()
