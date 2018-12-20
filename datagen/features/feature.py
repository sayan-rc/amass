class feature:
    def __init__(self, jobid, resources, tests, jobs_since, tools, lastJobs, test_time=None,singleJobFeature=None, queue=None, node_count=None, procs=None, otherFeatures=None):
        self.jobid = jobid
	self.resources = resources
	self.tests = tests
	if test_time: self.test_time=test_time
	else: self.test_time=[]
	self.tools = tools
	self.jobs_since=jobs_since
	self.lastJobs = lastJobs
	if singleJobFeature: self.singleJobFeature = singleJobFeature
	else: self.singleJobFeature=[]
	if otherFeatures: self.other = otherFeatures
	else: self.other=[]
	if queue: self.queue=queue
	else: self.queue=[]
	if node_count: self.nc=node_count
	else: self.nc = []
	if procs: self.procs=procs
        else: self.procs = []

    def toList(self):
	feat=[]
	feat.append(self.jobid)
	feat.extend(self.resources)
	feat.extend(self.tools)
	feat.extend(self.tests)
	feat.extend(self.test_time)
	feat.extend(self.jobs_since)
	feat.extend(self.queue)
	feat.extend(self.nc)
	feat.extend(self.procs)
	feat.extend(self.singleJobFeature)
	feat.extend(self.other)
	feat.extend(self.lastJobs)
	return feat

    def printFeat(self):
	feat= self.toList()
	print feat
	
