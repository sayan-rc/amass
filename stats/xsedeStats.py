import psycopg2, pymysql
import sys
import numpy as np
import matplotlib.pyplot as plt 
	
class xsedeStats:
	def connect(self):
		xpwd = "SwmP884ilFb@Yh"
		try:
			self.conn = psycopg2.connect("host='lima.sdsc.optiputer.net' user='kritika' dbname='xdcdb' password="+xpwd)
			self.cursor = self.conn.cursor()
		except psycopg2.Error as err:
			sys.stderr.write("%s due to %s\n" % ('Unable to connect',err))
			exit(1)

	def disconnect(self):
		self.cursor.close()
		self.conn.close()
	
	def plotBar(self, l,f, xlabel, ylabel):
		bins=range(0,int(max(l)))
		y,x=np.histogram(l,bins=bins)
		su = int(f*sum(y))
		s,i=0,0
		while(s<su):
			s+=y[i]
			i+=1
		y=y[:i]
		x=x[:i]
		plt.ylabel(ylabel, fontsize=15)
		plt.xlabel(xlabel, fontsize=15)
		p = plt.bar(x,y,color='b')
		plt.show()

	def plotHistogtrams(self):
		fields = "resource_id, wallduration, nodecount, processors, queue"
		fromtables = "from jobs"
		condition = "where (resource_id=2792 OR resource_id=2796)"
		query = "SELECT " + fields + " " + fromtables + " " + condition
		self.cursor.execute(query)
	
		wallduration, nodecount, processors, queue = [],[],[],[]
		resource = {2792:0,2796:0}
		queue = {'normal':0,'shared':0,'global':0,'debug':0,'power':0,'vsmp':0}
		for (resource_id,wd,nc,proc,q) in self.cursor:
			resource[resource_id]+=1
			wallduration.append(wd/(60.0*60.0*24))
			nodecount.append(nc)
			processors.append(proc)
			queue[q]+=1
	
		self.plotBar(wallduration,1,'Wallduration','Number of jobs')
		self.plotBar(nodecount,0.99,'Nodecount','Number of jobs')
		self.plotBar(processors,0.98,'Processors','Number of jobs')

def main(argv):
	stats = xsedeStats()
	try:
		stats.connect()
		stats.plotHistogtrams()
	finally:
		stats.disconnect()

if __name__ == "__main__":
    sys.exit(main(sys.argv))
