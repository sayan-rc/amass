===============
Automated Monitoring AnalySis Service (AMASS)
===============

A science gateway is a community-developed set of tools, applications, and data collections that are integrated via a portal or a 
suite of applications. It provides easy, typically browser-based, access to supercomputers, software tools, and data repositories 
to allow researchers to focus on their scientific goals and less on the cyberinfrastructure. These gateways are fostering 
collaboration and exchange of ideas among thousands of researchers from multiple communities ranging from atmospheric science, 
astrophysics, chemistry, biophysics, biochemistry, earthquake engineering, geophysics, to neuroscience, and biology. However due 
to limited development and administrative personnel resources, science gateways often leverage only a small subset of the 
NSF-funded CI to mitigate the complexities involved with using multiple resource and services at scale in part due to software 
and hardware failures. Since many successful science gateways have had unprecedented growth in their user base and ever 
increasing datasets, increasing their usage of CI resources without introducing additional complexity would help them meet 
this demand.

In response to this need, we are building an Automated Monitoring AnalySis Service (AMASS) to provide a flexible and extensible 
service for automated analysis of monitoring data initially focused on science gateways. AMASS is based on 
machine learning techniques to analyze monitoring data for improving the reliability and 
operational efficiency of CI as well as scientific gateways. 

The code in this repository is described in the below paper:

Singh, K., Smallen, S., Tilak, S., and Saul, L. (2016) Failure analysis and prediction for the CIPRES science gateway. 
Concurrency Computat.: Pract. Exper., 28: 1971-1981. doi: `10.1002/cpe.3715 <https://doi.org/10.1002/cpe.3715>`_.


Prerequisites
===============
- Access to a gateway database and implement a **Gateway** class defined at `<amass/features/gateway/__init__.py>`_.  
  An example is provided for science gateways based on the `CIPRES <http://www.phylo.org/>`_ science gateway framework at 
  `<amass/features/gateway/cipres/__init__.py>`_.
- A local MySQL database to use for caching data


Installing
===============

Checkout the following branch from Bitbucket ::

   $ git clone git https://bitbucket.org/amass-ucsd/amass.git
   $ cd amass
   $ git fetch && git checkout extensible-features-branch

Create a config file based on the template `<etc/amass.cfg.template>`_.


Running
===============

The main script for this code is `<bin/amass>`_.  To see all commands, run ::

   $ ./bin/amass help
   amass: client for Automated Monitoring AnalysiS Service

   Usage:
   amass generate features  <features> <gateway> <startdate> <enddate> [refresh-features=False] [refresh-sources=] [jobid=] [loglevel=ERROR]
   amass generate prediction  <features> <gateway> [filtererrors=] [split=0.6] [loglevel=ERROR]
   amass help 
   amass list cached errors  <features> <gateway> [printerror=] [loglevel=ERROR]   
   amass list cached features  [names=False] [loglevel=ERROR]
   amass list cached sources  [loglevel=ERROR]  
   amass list features  [loglevel=ERROR]

To list the help for any command, just execute `./bin/amass <command> help'.  E.g., ::

   $ ./bin/amass generate features help
   
To execute the predictions, you will need to first generate the features from the data sources (currently Inca and the gateway).  
E.g.,  ::

   $ ./bin/amass generate features submit <gateway> "YYYY-MM-DD HH:MM:SS" "YYYY-MM-DD HH:MM:SS"
   
This will cache source data into a local MySQL database and generate the features that will be used for training and predictions.  
To test your model, you can run the predictions on known data -- specify the number of records to be used for 
training (e.g., 40,480 below) and the remaining records can be used to validate the dataset. :: 

   $ ./bin/amass generate prediction submit <gateway> training=40480 validate-model=true
   ('tp', 'tn', 'fp', 'fn')
   ('Train metrics', (47685, 223, 355, 81, 0.9863685255667715, 0.004612775111699487, 0.0073432070163825915, 0.0016754923051464504))
   ('Test metrics', (31787, 132, 254, 58, 0.9862244423070956, 0.004095436070863454, 0.007880611833328163, 0.00179950978871273))
   ('Accuracy', 0.9903198783779591)
   ('Precision(1)', 0.9920726569083362)
   ('Precision(0)', 0.6947368421052632)
   ('Recall(0)', 0.34196891191709844)
   
The above shows the accuracy of how well the model performed by displaying the true positives, true negatives, 
false positives, and false negatives produced by the model.  To run a prediction on a to be submitted job, create a 
json file (e.g., job.json) in the following format: ::

   {
      "RESOURCE": "<resourcename>",
      "TOOL_NAME": "<toolname>",
      "USERNAME": "<username>",
      "USER_SUBMIT_DATE": "YYYY-MM-DD HH:DD:SS",
      "TERMINATE_DATE": "YYYY-MM-DD HH:DD:SS",
   }
   
Note, that TERMINATE_DATE is an expected termination date of the job.  Then execute, ::

   ./bin/amass generate prediction submit <gateway> training=<num records to use for training> job=job.json

Customizing Features
===============

Currently, this code features are based of Inca test data and gateway data.  However, the code has been designed to be extensible.  
To add new features to be considered in the model, you must implement a **FeatureSource** class as defined in `<amass/features/__init__.py>`_.
One **FeatureSource** class should be implemented for every new source of data that you are adding to the machine learning
model.  For every new set of features you define, you will implement an **add_feature_<feature__name>(self, job_info)** function.
The add_feature functions take as input a job info object (see columns attribute in the **FeatureSource** class defined in
`<amass/features/gateway/__init__.py>`_) and returns an array of features that will be joined together with the other features
for a particular gateway job.  An example is provided for Inca features at `<amass/features/inca/__init__.py>`_. 

Then edit your etc/amass.cfg file and append your new source and feature to the 'features' section.  You can add any new feature
definition below or append the existing **submit** feature set.  E.g., ::

   [features]
   submit=gateway.resources, gateway.tools, inca.tests, mynewsource.newfeature


Running AMASS (future)
===============

We are in the process of enabling AMASS to run as a Django service.  The main branch of the code is unfortunately still in progress.