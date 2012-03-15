'''
Created on Sep 30, 2010

@author: vagos, ikons
'''
#!/usr/bin/env python
 

from Deamon import Daemon
#import Utils
import sys, os, time, logging
import EucaCluster, MonitorVms, DecisionMaker , OpenStackCluster, FSMDecisionMaker
import  HBase92Cluster, HBaseCluster, VoldemortCluster, CassandraCluster, RiakCluster

class MyDaemon(Daemon):
    
        def run(self):
           
            ## Install logger
            LOG_FILENAME = self.utils.install_dir+'/logs/Coordinator.log'
            self.my_logger = logging.getLogger('Coordinator')
            self.my_logger.setLevel(logging.DEBUG)
            
            handler = logging.handlers.RotatingFileHandler(
                          LOG_FILENAME, maxBytes=2*1024*1024, backupCount=5)
            formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")
            handler.setFormatter(formatter)
            self.my_logger.addHandler(handler)
            
            ## Return the environment with which the daemon is run
            self.my_logger.debug(os.environ)
            self.my_logger.debug(self.utils.bucket_name)

            ## Initialize the nosql cluster (default HBase cluster). Runs two instances, copies all
            ## necessary configuration files, starts all the daemons and returns the high level nosql
            ## cluster object.            
            self.eucacluster, self.nosqlCluster = self.initializeNosqlCluster()
            
            ## Initialize monitoring for all nodes in the cluster
            self.vmMonitor = MonitorVms.MonitorVms(self.nosqlCluster.cluster)
            
            ## Collect and print out all metrics for the cluster.
            allmetrics=self.vmMonitor.refreshMetrics()
            print "allmetrics: ", allmetrics
#            allmetrics=self.vmMonitor.refreshMetrics()
#            print "allmetrics2: ", allmetrics
            
#            self.decisionMaker = DecisionMaker.DecisionMaker(self.eucacluster, self.nosqlCluster, self.vmMonitor)
            self.decisionMaker = FSMDecisionMaker.FSMDecisionMaker(self.eucacluster, self.nosqlCluster, self.vmMonitor)
            
#	    christina: sleep for 10 mins first, then start taking decisions!
            #time.sleep(10*60)
            
            while True:
                time.sleep(30)
                # to time.sleep sets the time interval until coordinator checks ganglia stats again
                # run ganglia update stats
                allmetrics = self.vmMonitor.refreshMetrics()
#                self.my_logger.debug( str(len(allmetrics)) )
#                self.my_logger.debug( str(allmetrics) ) 
                self.decisionMaker.takeDecision(allmetrics)
            

        def initializeNosqlCluster(self):

            # Assume running when eucarc sourced 
            if self.utils.cloud_api_type == "EC2":
                eucacluster = EucaCluster.EucaCluster()
                self.my_logger.debug("Created EucalyptusCluster with EC2 API")
            if self.utils.cloud_api_type == "EC2_OS":
                eucacluster = OpenStackCluster.OpenStackCluster()
                self.my_logger.debug("Created OpenStackCluster with EC2 API and public ipv6 dnsname")
                
            instances = eucacluster.describe_instances()
            self.my_logger.debug("All user instances:" + str(instances))
            
            ## creates a new Hbase cluster
            nosqlcluster=None
            
            if self.utils.cluster_type == "HBASE":
                nosqlcluster = HBaseCluster.HBaseCluster(self.utils.cluster_name)
            if self.utils.cluster_type == "HBASE92":
                nosqlcluster = HBase92Cluster.HBase92Cluster(self.utils.cluster_name)
            if self.utils.cluster_type == "VOLDEMORT":
                nosqlcluster = VoldemortCluster.VoldemortCluster(self.utils.cluster_name)
            if self.utils.cluster_type == "CASSANDRA":
                nosqlcluster = CassandraCluster.CassandraCluster(self.utils.cluster_name)
            if self.utils.cluster_type == "RIAK":
                nosqlcluster = RiakCluster.RiakCluster(self.utils.cluster_name)
                
            instances = []
            if not eval(self.utils.reconfigure):
                self.my_logger.debug("Removing previous instance of cluster from db")
                self.utils.delete_cluster_from_db(self.utils.cluster_name)
                images = eucacluster.describe_images(self.utils.bucket_name)
                self.my_logger.debug("Found emi in db: " + str(images[0].id))
                instances = eucacluster.run_instances(images[0].id, None, None, None, self.utils.initial_cluster_size, self.utils.initial_cluster_size, self.utils.instance_type)
                self.my_logger.debug("Launched new instances: " + str(instances))
                instances = eucacluster.block_until_running(instances)
                self.my_logger.debug("Running instances: " + str(instances))
            else:
                instances.append(nosqlcluster.cluster[nosqlcluster.host_template+"master"])
                for i in range(1,len(nosqlcluster.cluster)):
                    instances.append(nosqlcluster.cluster[nosqlcluster.host_template+str(i)])
                self.my_logger.debug("Found old instances: " + str(instances))
                self.my_logger.debug("WARNING: Will block forever if they are not running.")
                eucacluster.block_until_running(instances)
                self.my_logger.debug("Running instances: " + str(instances))
            
            
            
            
            self.my_logger.debug(nosqlcluster.configure_cluster(instances, self.utils.hostname_template, eval(self.utils.reconfigure)))
            time.sleep(60)
            nosqlcluster.start_cluster()
            #nosqlcluster.start_cluster()
            return eucacluster, nosqlcluster

        def reload(self):
            ''' Replaces the decision maker to reload the configuration properties '''
#            self.decisionMaker = DecisionMaker.DecisionMaker(self.eucacluster, self.nosqlCluster, self.vmMonitor)
            self.decisionMaker = FSMDecisionMaker.FSMDecisionMaker(self.eucacluster, self.nosqlCluster, self.vmMonitor)
 
if __name__ == "__main__":
        daemon = MyDaemon('/tmp/daemon-example.pid')
        if len(sys.argv) == 2:
                if 'start' == sys.argv[1]:
                        daemon.start()
                elif 'stop' == sys.argv[1]:
                        daemon.stop()
                elif 'restart' == sys.argv[1]:
                        daemon.restart()
                elif 'reload' == sys.argv[1]:
                        daemon.reload()
                else:
                        print "Unknown command"
                        sys.exit(2)
                sys.exit(0)
        else:
                print "usage: %s start|stop|restart" % sys.argv[0]
                sys.exit(2)