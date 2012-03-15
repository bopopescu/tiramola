'''
Created on Oct 10, 2010

@author: vagos
'''

import logging.handlers, thread, threading, time, sys
import Utils

class DecisionMaker(object):
    '''
    This class takes input from monitorVms class on a periodic basis and adds or removes virtual cluster nodes
    based on user defined policies.
    '''

    def __init__(self, eucacluster, NoSQLCluster, VmMonitor):
        '''
        Constructor. EucaCluster is the object with which you can alter the 
        number of running virtual machines in Eucalyptus 
        NoSQLCluster contains methods to add or remove virtual machines from the virtual NoSQLCluster
        ''' 
        self.utils = Utils.Utils()
        self.eucacluster=eucacluster
        self.NoSQLCluster=NoSQLCluster
        self.VmMonitor = VmMonitor
        self.polManager =  PolicyManager("test",self.eucacluster, self.NoSQLCluster)
        self.acted = ["done"]
        self.runonce = "once"
        self.refreshMonitor = "refreshed"
        
        ## Install logger
        LOG_FILENAME = self.utils.install_dir+'/logs/Coordinator.log'
        self.my_logger = logging.getLogger('DecisionMaker')
        self.my_logger.setLevel(logging.DEBUG)
        
        handler = logging.handlers.RotatingFileHandler(
                      LOG_FILENAME, maxBytes=2*1024*1024, backupCount=5)
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")
        handler.setFormatter(formatter)
        self.my_logger.addHandler(handler)

        
    def takeDecision(self, allmetrics):
        '''
         this method reads allmetrics object created by MonitorVms and decides to change the number of participating
         virtual nodes.
        '''
        ## Take decision based on metrics
        action = "none"
        
        ## Unwrap metrics from config file and evaluate them (OR for add, AND for remove)
        ## Use sensitive add policy - if even one node has problems 
#        for thresname, thresvalue in self.utils.thresholds_add.items():
#            splits = thresvalue.split("_")
#            if (splits[0] == "high"):
#                for host in allmetrics.values():
#                    if host.has_key(thresname):
#                        if splits[1].startswith("%"):
#                            ## If the measure is percentile, then evaluate with the total metric from ganglia
#                            val = splits[1].replace("%","")
#                            if host.has_key(thresname.replace(thresname.split("_")[1],"total")):
#                                if (float(host[thresname])/float(host[thresname.replace(thresname.split("_")[1],"total")]) * 100) > float(val):
#                                    action = "add"
#                                    self.my_logger.debug("Action ADD NODE triggered by: " + thresname + " = " +  host[thresname])
#                        else:
#                            if float(host[thresname]) > float(splits[1]):
#                                action = "add"  
#                                self.my_logger.debug("Action ADD NODE triggered by: " + thresname + " = " +  host[thresname])
#                        
#            else:
#                for host in allmetrics.values():
#                    if host.has_key(thresname):
#                        if splits[1].startswith("%"):
#                            val = splits[1].replace("%","")
#                            if host.has_key(thresname.replace(thresname.split("_")[1],"total")):
#                                if (float(host[thresname])/float(host[thresname.replace(thresname.split("_")[1],"total")]) * 100) < float(val):
#                                    action = "add"
#                                    self.my_logger.debug("Action ADD NODE triggered by: " + thresname + " = " +  host[thresname])
#                        else:
#                            if float(host[thresname]) < float(splits[1]):
#                                action = "add"
#                                self.my_logger.debug("Action ADD NODE triggered by: " + thresname + " = " +  host[thresname])
        for host in allmetrics.values():
            votes = []
            for thresname, thresvalue in self.utils.thresholds_add.items():
                splits = thresvalue.split("_")
                if (splits[0] == "high"):
                    if host.has_key(thresname):
                        if splits[1].startswith("%"):
                            ## If the measure is percentile, then evaluate with the total metric from ganglia
                            val = splits[1].replace("%","")
                            if host.has_key(thresname.replace(thresname.split("_")[1],"total")):
                                if (float(host[thresname])/float(host[thresname.replace(thresname.split("_")[1],"total")]) * 100) > float(val):
#                                    action = "add"
                                    votes.append(thresname)
#                                    self.my_logger.debug("Action ADD NODE triggered by: " + thresname + " = " +  host[thresname])
                        else:
                            if float(host[thresname]) > float(splits[1]):
#                                action = "add"  
                                votes.append(thresname)
#                                self.my_logger.debug("Action ADD NODE triggered by: " + thresname + " = " +  host[thresname])
                        
                else:
                    if host.has_key(thresname):
                        if splits[1].startswith("%"):
                            val = splits[1].replace("%","")
                            if host.has_key(thresname.replace(thresname.split("_")[1],"total")):
                                if (float(host[thresname])/float(host[thresname.replace(thresname.split("_")[1],"total")]) * 100) < float(val):
#                                    action = "add"
                                    votes.append(thresname)
#                                    self.my_logger.debug("Action ADD NODE triggered by: " + thresname + " = " +  host[thresname])
                        else:
                            if float(host[thresname]) < float(splits[1]):
#                                action = "add"
                                votes.append(thresname)
#                                self.my_logger.debug("Action ADD NODE triggered by: " + thresname + " = " +  host[thresname])
#            print "votes=", votes
            if len(votes) >= len(self.utils.thresholds_add):
                action = "add"
                self.my_logger.debug("Action ADD NODE triggered by:" + str(votes))
        
        ## To remove we let each host vote and look for consensus
        votes = []
        for thresname, thresvalue in self.utils.thresholds_remove.items():
            splits = thresvalue.split("_")
            if (splits[0] == "high"):
                for host in allmetrics.values():
                    if host.has_key(thresname):
                        if splits[1].startswith("%"):
                            ## If the measure is percentile, then evaluate with the total metric from ganglia
                            val = splits[1].replace("%","")
                            if host.has_key(thresname.replace(thresname.split("_")[1],"total")):
                                if (float(host[thresname])/float(host[thresname.replace(thresname.split("_")[1],"total")]) * 100) > float(val):
                                    votes.append(thresname)
                        else:
                            if float(host[thresname]) > float(splits[1]):
                                votes.append(thresname) 
            else:
                for host in allmetrics.values():
                    if host.has_key(thresname):
                        if splits[1].startswith("%"):
                            val = splits[1].replace("%","")
                            if host.has_key(thresname.replace(thresname.split("_")[1],"total")):
                                if (float(host[thresname])/float(host[thresname.replace(thresname.split("_")[1],"total")]) * 100) < float(val):
                                    votes.append(thresname)
                        else:
                            if float(host[thresname]) < float(splits[1]):
                                votes.append(thresname)
                                
#        print "Thresh_remove:", self.utils.thresholds_remove
#        print "VotesNum:", len(votes), " AllmetricsNum:", len(allmetrics), " ClusterLength:", len(self.NoSQLCluster.cluster)
        sys.stdout.flush()
        
        ## Don't start more instances than max cluster size
        if len(self.NoSQLCluster.cluster) >= int(self.utils.max_cluster_size):
            action = "none"
                            
        ## if everyone has voted to remove then remove if the cluster size is bigger than the original cluster size
        if len(votes) >= (len(self.NoSQLCluster.cluster)*len(self.utils.thresholds_remove)) and (len(self.NoSQLCluster.cluster) > int(self.utils.initial_cluster_size)):
            action = "remove"
            self.my_logger.debug("Action REMOVE NODE triggered. All conditions were met by all participating nodes.")
        
#        for host in allmetrics:
#            if ((host['mem_free']/host['mem_total']) * 100) < 90 or host['load_one'] > 2 or host['disk_free'] < 47 : 
#                action = "add"
        
        ## Time to act!
        self.my_logger.debug("Taking decision with acted: " + str(self.acted))
        if self.acted[len(self.acted)-1] == "done" :
            ## start the action as a thread
            thread.start_new_thread(self.polManager.act, (action,self.acted))
            self.my_logger.debug("Action undertaken: " + str(action))
            if not self.refreshMonitor.startswith("refreshed"):
                self.VmMonitor.configure_monitoring()
                self.refreshMonitor = "refreshed"
        else: 
            ## Action still takes place so do nothing
            self.my_logger.debug("Waiting for action to finish: " +  str(action) + str(self.acted))
            self.refreshMonitor = "not refreshed"
            
        action = "none"
        
#        if self.runonce :
#            threading.Timer(1500 , self.polManager.act("add",self.acted))
#            threading.Timer(3000 , self.polManager.act("add",self.acted))
#            self.refreshMonitor = "not refreshed"
#            self.runonce = None
        return True
    
class PolicyManager(object):
    '''
    This class manages and abstracts the policies that Decision Maker uses. 
    '''


    def __init__(self, policyDescription, eucacluster, NoSQLCluster):
        '''
        Constructor. Requires a policy description that sets the policy. 
        ''' 
        self.utils = Utils.Utils()
        self.pdesc = policyDescription
        self.eucacluster=eucacluster
        self.NoSQLCluster=NoSQLCluster
        
        ## Install logger
        LOG_FILENAME = self.utils.install_dir+'/logs/Coordinator.log'
        self.my_logger = logging.getLogger('PolicyManager')
        self.my_logger.setLevel(logging.DEBUG)
        
        handler = logging.handlers.RotatingFileHandler(
                      LOG_FILENAME, maxBytes=2*1024*1024, backupCount=5)
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")
        handler.setFormatter(formatter)
        self.my_logger.addHandler(handler)

    def act (self, action, acted):
        self.my_logger.debug("Taking decision with acted: " + str(acted))
        if self.pdesc == "test":
            if action == "add":
                images = self.eucacluster.describe_images(self.utils.bucket_name)
                self.my_logger.debug("Found emi in db: " + str(images[0].id))
                ## Launch as many instances as are defined by the user
                instances = self.eucacluster.run_instances(images[0].id, None, None, None, self.utils.add_nodes, self.utils.add_nodes, self.utils.instance_type)
                self.my_logger.debug("Launched new instance/instances: " + str(instances))
                acted.append("paparia")
                instances = self.eucacluster.block_until_running(instances)
                self.my_logger.debug("Running instances: " + str(instances))
                self.my_logger.debug(self.NoSQLCluster.add_nodes(instances))
                ## Make sure nodes are running for a reasonable amount of time before unblocking
                ## the add method
                time.sleep(300)
                acted.pop() 
            if action == "remove":
                acted.append("paparia")
                ## remove last node and terminate the instance
                for hostname, host in self.NoSQLCluster.cluster.items():
                    if hostname.replace(self.NoSQLCluster.host_template, "") == str(len(self.NoSQLCluster.cluster)-1):
                        self.NoSQLCluster.remove_node(hostname)
                        if self.utils.cluster_type == "CASSANDRA":
                            time.sleep(120)
                        if self.utils.cluster_type == "HBASE":
                            time.sleep(30)
                        self.eucacluster.terminate_instances([host.id])
                        break
                    
                ## On reset to original cluster size, restart the servers
#                if (len(self.NoSQLCluster.cluster) == int(self.utils.initial_cluster_size)):
#                    self.NoSQLCluster.stop_cluster()
#                    self.NoSQLCluster.start_cluster()
                    
                acted.pop() 
#            if not action == "none":
#                self.my_logger.debug("Starting rebalancing for active cluster.")
#                self.NoSQLCluster.rebalance_cluster()
            
        action = "none"
            
