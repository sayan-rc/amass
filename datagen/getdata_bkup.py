import psycopg2, pymysql, sys

class getData:
    def __init__(self, xsedeuser, xsedepass, ciprespass, localdb, localpass, localuser, runonlima):
            self.localdb = localdb
            self.localpass = localpass
            self.localuser = localuser
            self.xsedeuser = xsedeuser
            self.xsedepass = xsedepass
            self.ciprespass = ciprespass       
            self.lima = runonlima

    def Connect(self):
        #connect to cipres and xsede db, also to the local db
    	try:
            if self.lima:	
                self.xdcdbConn=psycopg2.connect(database="xdcdb")
            else:
                self.xdcdbConn=psycopg2.connect(database="xdcdb",host="lima.sdsc.optiputer.net",user=self.xsedeuser,password=self.xsedepass)
            self.cipresConn=pymysql.connect(host='mysql2.sdsc.edu',port=3312,user='readonly',passwd=self.ciprespass,db='cipres')
            if self.localpass:
                self.localConn=pymysql.connect(user=self.localuser,passwd=self.localpass)
            else:
                self.localConn=pymysql.connect(user=self.localuser)
    	except pymysql.Error as err:
    		sys.stderr.write("%s\n" % str(err))
    
    def Disconnect(self):
        #disconnect from all the dbs
		self.xdcdbConn.close()
		self.cipresConn.close()
		self.localConn.close()
            
    def createLocalDB(self):
    	localCursor = self.localConn.cursor()
        try:
            #create local database
            localCursor.execute("CREATE DATABASE IF NOT EXISTS " + self.localdb)
            self.localConn.commit()
    	finally:
    		#close the connection
    		localCursor.close()
        
    def getCipresData(self, startDate, endDate):
    	cipresCursor = self.cipresConn.cursor()
    	localCursor = self.localConn.cursor()
    	try:
            #use the given database
            localCursor.execute("USE " + self.localdb)
            
            #get data from cipres
            fields = ("job_stats.jobhandle,resource,tool_id,date_entered,date_submitted,"
            "date_terminated,name,remote_job_id,value")
            fromtables = "from job_stats, job_events"
            condition = ("where job_stats.jobhandle=job_events.jobhandle AND"
            " (job_events.name=\'FINISHED\' OR job_events.name=\'FAILED\') AND "
            "(resource='GORDON' or resource='TRESTLES') AND date_entered>"+startDate)
            if endDate: condition+= " and date_entered<"+endDate
            query = "SELECT " + fields + " " + fromtables + " " + condition
            print query
            cipresCursor.execute(query)
		
            #drop local table if exists
            drop_table = "DROP TABLE IF EXISTS event_stats"
            localCursor.execute(drop_table)

            #create a local table
            create_table = ("CREATE TABLE event_stats (JOBHANDLE VARCHAR(255) NOT NULL,"
            " RESOURCE varchar(100) NULL, TOOL_ID varchar(100) NULL,"
            " DATE_ENTERED datetime NULL, DATE_SUBMITTED datetime  NULL,"
            " DATE_TERMINATED datetime  NULL, NAME VARCHAR(100) NULL,"
            " REMOTE_JOB_ID varchar(1023)  NULL, VALUE longtext NULL, PRIMARY KEY(JOBHANDLE))")
            localCursor.execute(create_table)
		
            #insert cipres data into the new table
            insert = "INSERT INTO event_stats (JOBHANDLE,RESOURCE,TOOL_ID,DATE_ENTERED,DATE_SUBMITTED,DATE_TERMINATED,NAME,REMOTE_JOB_ID,VALUE) VALUES ("
            count = 0
            for (jobhandle,resource, tool_id, date_entered, date_submitted, date_terminated, name, remote_job_id, value) in cipresCursor:
                count += 1
                jh, res, tid, n = '\''+jobhandle+'\',', '\''+resource+'\',', '\''+tool_id+'\',', '\''+name+'\','
                if not date_entered: de = 'NULL,'
                else: de = '\''+str(date_entered)+'\','
                if not date_submitted: ds = 'NULL,'
                else: ds = '\''+str(date_submitted)+'\','
                if not date_terminated: dt = 'NULL,'
                else: dt = '\''+str(date_terminated)+'\','	
                if not remote_job_id: rid = 'NULL,'
                else: rid = '\''+remote_job_id+'\','
                if not value: v = 'NULL'
                else: v = '\''+value+'\''
                iquery = (insert + jh + res + tid + de + ds + dt + n + rid + "%s" + ")")
                localCursor.execute(iquery,v)
                if(count%100000==0): sys.stdout.write("Queried %d cipres entries.\n" % count)
            sys.stdout.write("Queried %d cipres entries.\n" % count)

            #commit the changes to the local database
            self.localConn.commit()
    	finally:
    		#close the connections
    		cipresCursor.close()
    		localCursor.close()
            
    def getXDCDBData(self, startDate, endDate):
    	xdcdbCursor = self.xdcdbConn.cursor()
    	localCursor = self.localConn.cursor()
        try:
            #use the given database
            localCursor.execute("USE " + self.localdb)
            
            #get data from xdcdb
            fields = "resource_id, local_jobid, start_time at time zone 'US/Pacific', end_time at time zone 'US/Pacific', submit_time at time zone 'US/Pacific', wallduration, nodecount, processors, queue, local_charge"
            fromtables = "from jobs"
            condition = "where (resource_id=2796 or resource_id=2792) AND submit_time>"+startDate
            if endDate: condition+= " and submit_time<"+endDate
            query = "SELECT " + fields + " " + fromtables + " " + condition
            print query
            xdcdbCursor.execute(query)

            #drop local table if exists 
            drop_table = "DROP TABLE IF EXISTS xsede"
            localCursor.execute(drop_table)
		
            #create table
            create_table = ("CREATE TABLE xsede (resource_id bigint(20) NOT NULL,"
            "local_jobid varchar(255) NOT NULL, start_time timestamp  NOT NULL DEFAULT '0000-00-00 00:00:00',"
            "end_time timestamp  NOT NULL DEFAULT '0000-00-00 00:00:00', submit_time timestamp NOT NULL DEFAULT"
            " '0000-00-00 00:00:00', wallduration bigint(20) NOT NULL, nodecount bigint(20) NOT NULL,"
            "processors bigint(20) NULL, queue text(100) NULL, local_charge decimal NULL)")
            localCursor.execute(create_table)
	
    		#insert xdcdb data into the new table
            count = 0
            insert = "INSERT INTO xsede (resource_id,local_jobid,start_time,end_time,submit_time,wallduration,nodecount,processors,queue,local_charge) VALUES ("
            for (resource_id,local_jobid,start_time,end_time,submit_time,wallduration,nodecount,processors,queue,local_charge) in xdcdbCursor:
                count += 1
                rid, lid, wd, nc, proc, q, lc = '\''+str(resource_id)+'\',', '\''+local_jobid+'\',', '\''+str(wallduration)+'\',', '\''+str(nodecount)+'\',', '\''+str(processors)+'\',', '\''+queue+'\'', ', '+str(local_charge)
                sttime = '\''+str(start_time)+'\','
                etime = '\''+str(end_time)+'\','
                sutime = '\''+str(submit_time)+'\','	
                iquery = insert + rid + lid + sttime + etime + sutime + wd + nc + proc + q + lc + ")"
                localCursor.execute(iquery)
                if(count%100000==0): sys.stdout.write("Queried %d xdcdb entries.\n" % count)
    		#commit the changes to the local database	
            sys.stdout.write("Queried %d xdcdb entries.\n" % count)
            self.localConn.commit()
    	finally:
    		xdcdbCursor.close()
    		localCursor.close()
            
    def joinTables(self):
    	localCursor = self.localConn.cursor()
    	try:
            #use the given database
            localCursor.execute("USE " + self.localdb)
            
            #drop tables if they exist
            localCursor.execute("DROP TABLE IF EXISTS gordon")
            localCursor.execute("DROP TABLE IF EXISTS trestles")
            localCursor.execute("DROP TABLE IF EXISTS cipres_xsede")

            #create separate temporary tables for gordon and trestles
            fields="resource_id,local_jobid,start_time,end_time,wallduration,nodecount,processors,queue,local_charge"
            fromtables = "from xsede"
            gordon_cond = "where resource_id=2796"
            trestles_cond = "where resource_id=2792"
            gordon_query="CREATE TABLE gordon SELECT "+fields+" "+fromtables+" "+gordon_cond
            trestles_query="CREATE TABLE trestles SELECT "+fields+" "+fromtables+" "+trestles_cond
            localCursor.execute(gordon_query)
            localCursor.execute(trestles_query)

            #update the local_jobid field of both the tables
            change_field="set local_jobid=substring(local_jobid,1,locate('.',local_jobid)-1)"
            update_gordon="update gordon "+change_field
            update_trestles="update trestles "+change_field
            localCursor.execute(update_gordon)
            localCursor.execute(update_trestles)
	
            #make local_jobid the primary key
            update_field="add primary key (local_jobid)"
            update_gordon="alter table gordon "+update_field
            update_trestles="alter table trestles "+update_field
            localCursor.execute(update_gordon)
            localCursor.execute(update_trestles)
		
            #create a join of gordon and xsede into a new table
            fields=("jobhandle,resource,value,tool_id,start_time,end_time"
            ",wallduration,nodecount,processors,queue,name,local_charge")
            from_tables="FROM gordon,event_stats "
            where="where local_jobid=remote_job_id and resource="
            create_table="CREATE TABLE cipres_xsede SELECT "+fields+" "+from_tables+where+"'gordon'"
            localCursor.execute(create_table)
		
            #add join of trestles and xsede into the new table
            from_tables="FROM trestles,event_stats "
            insert_into="INSERT INTO cipres_xsede ("+fields+")"+" SELECT "+fields+" "+from_tables+where+"'trestles'"	
            localCursor.execute(insert_into)
		
            #drop the temporary tables
            #localCursor.execute("DROP TABLE IF EXISTS gordon")
            #localCursor.execute("DROP TABLE IF EXISTS trestles")
		
            #commit the changes to the local database
            self.localConn.commit()
    	finally:
    		#close the connection
    		localCursor.close()
