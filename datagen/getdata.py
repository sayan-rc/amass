import psycopg2, pymysql, re, sys

class getData:
    def __init__(self, config, datadir):
        self.config = config
        self.datadir = datadir
        self.gateway_resources = re.split("\s*,\s*", self.config.get("resources", "gateway"))
        self.xsede_resource_ids = re.split("\s*,\s*", self.config.get("resources", "xsede_ids"))
        self.db_resources = re.split("\s*,\s*", self.config.get("resources", "db"))

    def Connect(self):
        #connect to cipres and xsede db, also to the local db
        xdcdb_attrs = ""
        for name, value in self.config.items("xsede"):
            xdcdb_attrs = "%s %s=%s" % (xdcdb_attrs, name, value)
        db_attrs = {}
        for db in ['gateway', 'amassdb']:
            db_attrs[db] = {}
            for name, value in self.config.items(db):
                if name == 'port':
                     db_attrs[db][name] = int(value)
                else:
                     db_attrs[db][name] = value
        try:
            self.xdcdbConn = psycopg2.connect(xdcdb_attrs)
            self.gatewayConn=pymysql.connect(**db_attrs['gateway'])
            self.localConn=pymysql.connect(**db_attrs['amassdb'])
        except pymysql.Error as err:
            sys.stderr.write("%s\n" % str(err))
            sys.exit(1)

    def Disconnect(self):
        #disconnect from all the dbs
      try:
          self.xdcdbConn.close()
      except:
          pass
      try:
          self.gatewayConn.close()
      except:
          pass
      try:
          self.localConn.close()
      except:
          pass

    def createLocalDB(self):
        localCursor = self.localConn.cursor()
        try:
            #create local database
            localCursor.execute("CREATE DATABASE IF NOT EXISTS %s" % self.config.get('amass', 'database'))
            self.localConn.commit()
        except:
            pass
        finally:
            #close the connection
            localCursor.close()

    def getGatewayData(self, startDate, endDate, mode):
        cipresCursor = self.gatewayConn.cursor()
        localCursor = self.localConn.cursor()
        try:
            #use the given database
            localCursor.execute("USE %s" % self.config.get("amassdb", "database"))

            #get data from cipres
            resource_str='(resource in (%s))' % ",".join(["'%s'"%x for x in self.gateway_resources])
            fields = ("job_stats.jobhandle,resource,tool_id,date_entered,date_submitted,"
            "date_terminated,name,remote_job_id,value")
            fromtables = "from job_stats, job_events"
            condition0 = ("where job_stats.jobhandle=job_events.jobhandle AND"
            " (job_events.name=\'FINISHED\' OR job_events.name=\'FAILED\') AND "
            +resource_str + " AND date_entered>" + startDate)
            if endDate: condition0+= " and date_entered<"+endDate
            condition1 = ("where job_stats.jobhandle=job_events.jobhandle AND"
            " (job_events.name=\'FINISHED\' OR job_events.name=\'FAILED\') AND "
            +resource_str + " AND date_submitted>" + startDate)
            if endDate: condition1+= " and date_submitted<"+endDate
            if mode==0:
                query = "SELECT " + fields + " " + fromtables + " " + condition0
            else:
                query = "SELECT " + fields + " " + fromtables + " " + condition1
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

    def getXDCDBData(self, startDate, endDate, mode):
        xdcdbCursor = self.xdcdbConn.cursor()
        localCursor = self.localConn.cursor()
        try:
            #use the given database
            localCursor.execute("USE %s" % self.config.get("amassdb", "database"))

            #get data from xdcdb
            resource_id_str='(resource_id in (%s))' % ",".join(["'%s'"%x for x in self.xsede_resource_ids])
            fields = "resource_id, local_jobid, start_time at time zone 'US/Pacific', end_time at time zone 'US/Pacific', submit_time at time zone 'US/Pacific', wallduration, nodecount, processors, queue, local_charge"
            fromtables = "from jobs"
            condition0 = "where " + resource_id_str + "AND start_time>" + startDate
            if endDate: condition0+= " and start_time<"+endDate
            condition1 = "where" + resource_id_str + "AND submit_time>" + startDate
            if endDate: condition1+= " and submit_time<"+endDate
            if mode==0:
                query = "SELECT " + fields + " " + fromtables + " " + condition0
            else:
                query = "SELECT " + fields + " " + fromtables + " " + condition1
            print query
            xdcdbCursor.execute(query)

            #drop local table if exists 
            drop_table = "DROP TABLE IF EXISTS xsede"
            localCursor.execute(drop_table)

            #create table
            create_table = ("CREATE TABLE xsede (resource_id bigint(20) NOT NULL,"
            "local_jobid varchar(255) NOT NULL, start_time timestamp DEFAULT '0000-00-00 00:00:00',"
            "end_time timestamp DEFAULT '0000-00-00 00:00:00', submit_time timestamp DEFAULT"
            " '0000-00-00 00:00:00', wallduration bigint(20) DEFAULT 0, nodecount bigint(20) DEFAULT 0,"
            "processors bigint(20) DEFAULT 0, queue text(100) NULL, local_charge decimal NULL)")
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
            localCursor.execute("USE %s" % self.config.get("amassdb", "database"))

            #drop tables if they exist
            for r in self.db_resources:
                localCursor.execute("DROP TABLE IF EXISTS "+r)
            localCursor.execute("DROP TABLE IF EXISTS gateway_xsede")

            #create separate temporary tables for resources
            fields="resource_id,local_jobid,start_time,submit_time, end_time,wallduration,nodecount,processors,queue,local_charge"
            fromtables = "from xsede"
            for i, resource in enumerate(self.db_resources):
                condition="where resource_id="+self.xsede_resource_ids[i]
                q="CREATE TABLE "+resource+" SELECT "+fields+" "+fromtables+" "+condition
		sys.stdout.write(q+"\n")
                localCursor.execute(q)

            #update the local_jobid field of all the tables except comet
            change_field="set local_jobid=substring(local_jobid,1,locate('.',local_jobid)-1)"
            for i in xrange(len(self.db_resources)-1):
                update_r="update "+self.db_resources[i]+" "+change_field
                localCursor.execute(update_r)

	    #delete irrelevant entries
	    where_d="where local_jobid=' '"
	    for r in self.db_resources:
		delete_r="delete from "+ r +" "+where_d
		localCursor.execute(delete_r)

            #make local_jobid the primary key
            update_field="add primary key (local_jobid)"
            for r in self.db_resources:
                update_r="alter table "+r+" "+update_field
                localCursor.execute(update_r)

            #create a join of resource 0 and xsede into a new table
            fields=("local_jobid,jobhandle,resource,value,tool_id, date_entered, date_submitted, date_terminated"
            ",wallduration,nodecount,processors,queue,name,local_charge")
	    fields1= ("jobhandle, resource, value, tool_id, name, date_entered, date_submitted, date_terminated")
            from_tables="FROM "+self.db_resources[0]+",event_stats "
	    from_table1="FROM event_stats "
            where="where local_jobid=remote_job_id and resource="
	    where1="where remote_job_id is NULL and resource="
            create_table="CREATE TABLE gateway_xsede SELECT "+fields+" "+from_tables+where+"\'"+self.db_resources[0]+"\'"
	    insert1="INSERT IGNORE INTO gateway_xsede ("+fields1+")"+" SELECT "+fields1+" "+from_table1+where1+"\'"+self.db_resources[0]+"\'"
	    sys.stdout.write(create_table+"\n")
            localCursor.execute(create_table)
	    #sys.stdout.write(insert1+"\n")
	    #localCursor.execute(insert1)
	    
	    
            #add join of other resources and xsede into the new table
            for i in xrange(1,len(self.gateway_resources)):
                from_tables="FROM "+self.db_resources[i]+",event_stats "
                insert_into="INSERT IGNORE INTO gateway_xsede ("+fields+")"+" SELECT "+fields+" "+from_tables+where+"\'"+self.db_resources[i]+"\'"
		insert2="INSERT IGNORE INTO gateway_xsede ("+fields1+")"+" SELECT "+fields1+" "+from_table1+where1+"\'"+self.db_resources[i]+"\'"
	        sys.stdout.write(insert_into+"\n")
                localCursor.execute(insert_into)
	        #sys.stdout.write(insert2+"\n")
            	#localCursor.execute(insert2)
	    
            #drop the temporary tables
            #localCursor.execute("DROP TABLE IF EXISTS gordon")
            #localCursor.execute("DROP TABLE IF EXISTS trestles")

            #commit the changes to the local database
            self.localConn.commit()
        finally:
                #close the connection
                localCursor.close()
