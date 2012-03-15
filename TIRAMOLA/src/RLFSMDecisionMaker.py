'''
Created on Sep 21, 2011

@author: christina
'''

import fuzz, logging, math, time, thread
import Utils, operator, os, scipy

class RLFSMDecisionMaker():
    def __init__(self, eucacluster, NoSQLCluster, VmMonitor):
        '''
        Constructor. EucaCluster is the object with which you can alter the 
        number of running virtual machines in OpenStack 
        NoSQLCluster contains methods to add or remove virtual machines from the virtual NoSQLCluster
        ''' 
        self.utils = Utils.Utils()
        self.eucacluster = eucacluster
        self.NoSQLCluster = NoSQLCluster
        self.VmMonitor = VmMonitor
        self.polManager =  PolicyManager("test",self.eucacluster, self.NoSQLCluster)
        self.acted = ["done"]
        self.runonce = "once"
        self.refreshMonitor = "refreshed"
        cluster_size = len(self.utils.get_cluster_from_db(self.utils.cluster_name))
        self.currentState = str(cluster_size)
        self.nextState = str(cluster_size)
        self.debug = False
        # A dictionary that will remember rewards and metrics in states previously visited
        self.memory = {}
        
        for i in range(int(self.utils.initial_cluster_size), int(self.utils.max_cluster_size)+1):
            self.memory[str(i)]['V'] = 0 # placeholder for rewards and metrics
        
        ## Install logger
        LOG_FILENAME = self.utils.install_dir+'/logs/Coordinator.log'
        self.my_logger = logging.getLogger('RLFSMDecisionMaker')
        self.my_logger.setLevel(logging.DEBUG)
        
        handler = logging.handlers.RotatingFileHandler(
                      LOG_FILENAME, maxBytes=2*1024*1024, backupCount=5)
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")
        handler.setFormatter(formatter)
        self.my_logger.addHandler(handler)
        
    def __init__(self, cluster_size):
        '''
        Constructor. Dummy constructor used for running tests for the decision making with simulation!
        ''' 
        self.utils = Utils.Utils()
        #self.eucacluster = eucacluster
        #self.NoSQLCluster = NoSQLCluster
        #self.VmMonitor = VmMonitor
        #self.polManager =  PolicyManager("debug")
        self.acted = ["done"]
        self.runonce = "once"
        self.refreshMonitor = "refreshed"
        #cluster_size = len(self.utils.get_cluster_from_db(self.utils.cluster_name))
        self.currentState = str(cluster_size)
        self.nextState = str(cluster_size)
        self.debug = True
        # A dictionary that will remember rewards and metrics in states previously visited
        self.memory = {}
                
        for i in range(int(self.utils.initial_cluster_size), int(self.utils.max_cluster_size)+1):
            self.memory[str(i)] = {}
            self.memory[str(i)]['V'] = None # placeholder for rewards and metrics
            self.memory[str(i)]['r'] = None
        
        ## Install logger
        LOG_FILENAME = self.utils.install_dir+'/logs/Coordinator.log'
        self.my_logger = logging.getLogger('RLFSMDecisionMaker')
        self.my_logger.setLevel(logging.DEBUG)
        
        handler = logging.handlers.RotatingFileHandler(
                      LOG_FILENAME, maxBytes=2*1024*1024, backupCount=5)
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")
        handler.setFormatter(formatter)
        self.my_logger.addHandler(handler)
        
        # Load any previous statics.
        self.measurementsFile = self.utils.install_dir+'/logs/measurements.txt'
        self.sumMetrics = {}
        meas = open(self.measurementsFile, 'a')
        if os.stat(self.measurementsFile).st_size == 0:
            # The file is empty, set the headers for each column.
            meas.write('State\t\tThroughput\t\tLatency\n')
        else :
            # Read the measurements saved in the file.
            meas.close()
            meas = open(self.measurementsFile, 'r')
            
            meas.next() # Skip the first line with the headers of the columns
            for line in meas:
                m = line.split('\t\t')
                self.addMeasurement(m)
        
        meas.close()
        
        for (key, met) in self.sumMetrics.iteritems():
            self.my_logger.debug("Average metrics read from file for state: "+ key +" num of measurements: "+ str(met['divide_by']) +
                                 " av. throughput: "+ str(met['throughput']/met['divide_by']) +" av. latency: " +
                                 str(met['latency']/met['divide_by']))
    
    # param metrics: array The metrics to store. An array containing [state, throughput, latency]
    # param writeFile: boolean If set write the measurement in the txt file
    def addMeasurement(self, metrics, writeFile=False):
        if not self.sumMetrics.has_key(metrics[0]):
            # Save the metric with the state as key
            self.sumMetrics[metrics[0]] = {'throughput': 0.0, 'latency': 0.0, 'divide_by': 0}
        
        self.sumMetrics[metrics[0]] = {'throughput': self.sumMetrics[metrics[0]]['throughput'] + float(metrics[1]), 
                                 'latency': self.sumMetrics[metrics[0]]['latency'] + float(metrics[2]),
                                 'divide_by': self.sumMetrics[metrics[0]]['divide_by'] + 1}
        if writeFile:
            ms = open(self.measurementsFile, 'a')
            ms.write(str(metrics[0])+'\t\t'+str(metrics[1])+'\t\t'+str(metrics[2])+'\n')
            ms.close()
            
    # param state: string Get the average metrics (throughput, latency) for this state.
    # return a dictionary with the averages
    def getAverages(self, state):
        averages = {}
        if self.sumMetrics.has_key(state):
            averages['throughput'] = float(self.sumMetrics[state]['throughput']/self.sumMetrics[state]['divide_by'])
            averages['latency'] = float(self.sumMetrics[state]['latency']/self.sumMetrics[state]['divide_by'])
            
            self.my_logger.debug("GETAVERAGES Average metrics for state: "+ state +" num of measurements: "+ str(self.sumMetrics[state]['divide_by']) +
                                 " av. throughput: "+ str(averages['throughput']) +" av. latency: " +
                                 str(averages['latency']))
        return averages
            
    def takeDecision(self, rcvallmetrics):
        '''
         this method reads allmetrics object created by MonitorVms and decides to change the number of participating
         virtual nodes.
        '''
        ## Take decision based on metrics
        action = "none"
        
        allmetrics = rcvallmetrics.copy()
        self.my_logger.debug( "TAKEDECISION state: " + str(self.currentState))
        if not allmetrics.has_key('inlambda'):
            allmetrics['inlambda'] = 0
            
        if not allmetrics.has_key('throughput'):
            allmetrics['throughput'] = 0
            
        if not allmetrics.has_key('latency'):
            allmetrics['latency'] = 0
            
        if not allmetrics.has_key('cpu'):
            allmetrics['cpu'] = 0
        
        if not self.debug :
            ## Aggreggation of YCSB client metrics
            clients = 0
            nodes = 0
            for host in allmetrics.values():
                if isinstance(host,dict):
                    if host.has_key("ycsb_LAMDA_1"):
                        for key in host.keys():
                            if key.startswith('ycsb_LAMDA'):
                                allmetrics['inlambda'] += float(host[key])
                            if key.startswith('ycsb_THROUGHPUT'):
                                allmetrics['throughput'] += float(host[key])
                            if key.startswith('ycsb_READ') or key.startswith('ycsb_UPDATE') or key.startswith('ycsb_RMW') or key.startswith('ycsb_INSERT'):
                                allmetrics['latency'] += float(host[key])
                                if host[key] > 0:
                                    clients += 1
                    for key in host.keys():
                        if key.startswith('cpu_nice') or key.startswith('cpu_wio') or key.startswith('cpu_user') or key.startswith('cpu_system'):
                            allmetrics['cpu'] += float(host[key])
                    nodes += 1
                                
            try: 
                allmetrics['latency'] = allmetrics['latency'] / clients
            except:
                allmetrics['latency'] = 0
            
            try: 
                allmetrics['cpu'] = allmetrics['cpu'] / nodes
            except:
                allmetrics['cpu'] = 0
        
        self.my_logger.debug( "TAKEDECISION allmetrics: " + str(allmetrics))
        
        
#        self.my_logger.debug( "trans" + self.utils.trans_cost)
#        self.my_logger.debug( "gain" + self.utils.gain)
        #1. Save the current metrics in memory.
        self.addMeasurement([str(self.currentState), allmetrics['inlambda'], allmetrics['latency']], True)
        #self.getAverages(str(self.currentState))
        
        states = fuzz.fset.FuzzySet()
        
        self.memory[self.currentState]['allmetrics'] = allmetrics
        cur_inlambda = allmetrics['inlambda']
        cur_latency = allmetrics['latency']
        # Create the graph using the values in memory if any, or defaults
        for i in range(int(self.utils.initial_cluster_size), int(self.utils.max_cluster_size)+1):
            allmetrics['max_throughput'] = float(i) * float(self.utils.serv_throughput)
            allmetrics['num_nodes'] = i
            av = self.getAverages(str(i))
            if len(av) > 0:
                # Dangerous???
                allmetrics['inlambda'] = av['throughput']
                allmetrics['latency'] = av['latency']
                # Been in this state before, use the measurements
                self.memory[str(i)]['r'] = eval(self.utils.gain, allmetrics)
                #self.memory[str(i)]['V'] = self.memory[str(i)]['r'] +
                states.add(fuzz.fset.FuzzyElement(str(i), self.memory[str(i)]['r']))
#                self.my_logger.debug("TAKEDECISION adding visited state "+ str(i) +" with gain "+ str(self.memory[str(i)]['r']))
            else:
                # No clue for this state use current measurements...reassign to be on the safe side.
                allmetrics['inlambda'] = cur_inlambda
                allmetrics['latency'] = cur_latency
                self.memory[str(i)]['r'] = eval(self.utils.gain, allmetrics)
                states.add(fuzz.fset.FuzzyElement(str(i), self.memory[str(i)]['r']))
#                self.my_logger.debug("TAKEDECISION adding state "+ str(i) +" with gain "+ str(self.memory[str(i)]['r']))
            
        v=[]

        for i in states.keys():
            v.append(i)
            
        v = set(v)
        
        stategraph = fuzz.fgraph.FuzzyGraph(viter = v, directed = True)
        
        #The subgraph we are interested in. It contains only the allowed transitions from the current state.
        from_node = max(int(self.utils.initial_cluster_size), (int(self.currentState) - int(self.utils.rem_nodes)))
        to_node = min((int(self.currentState) + int(self.utils.add_nodes)), int(self.utils.max_cluster_size))
#        self.my_logger.debug("TAKEDECISION creating graph from node: max("+ self.utils.initial_cluster_size +", "+
#                             self.currentState +" - "+ self.utils.rem_nodes +") = "+ str(from_node) +" to node: min("+
#                             self.currentState +" + "+ self.utils.add_nodes +", "+ self.utils.max_cluster_size +") = "+ str(to_node))
        for j in range(from_node, to_node+1):
            if j != int(self.currentState):
                # Connect nodes with allowed transitions from the current node.
                #connect(tail, head, mu)
                stategraph.connect(str(j), self.currentState, eval(self.utils.trans_cost, allmetrics))
#                self.my_logger.debug("TAKEDECISION connecting state "+ self.currentState +" with state "+ str(j))
                # Connect nodes with allowed transitions from node j.
                for k in range(max(int(self.utils.initial_cluster_size), j - int(self.utils.rem_nodes)), min(j + int(self.utils.add_nodes), int(self.utils.max_cluster_size))+1):
                    if k != j:
#                        self.my_logger.debug("TAKEDECISION connecting state "+ str(j) +" with state "+ str(k))
                        stategraph.connect(str(k), str(j), eval(self.utils.trans_cost, allmetrics))
                        
        #self.states.__getitem__(self.currentState).mu(eval(self.utils.gain, allmetrics))
#        allEdges = stategraph.edges()
#        for e in allEdges:
#            self.my_logger.debug("TAKEDECISION "+ str(e))
        #Calculate the V matrix for available transitions
        V = {}
        
        for s in range(from_node, to_node+1):
            # Get allowed transitions from this state. 
            if self.memory[str(s)]['r'] != None:
                self.my_logger.debug("TAKEDECISION allowed transitions for state: "+ str(s))
                #For each state s, we need to calculate the transitions allowed.
                allowed_transitions = stategraph.edges(head=str(s))
#                if (len(allowed_transitions) == 0):
#                    self.my_logger.debug("TAKEDECISION no edges with head: "+ str(s))
#                else :
#                    self.my_logger.debug("TAKEDECISION "+ str(len(allowed_transitions)) +" edges with head: "+ str(s))
                    
                #allowed_transitions.union(tails)
                Vs = []
                
                for t in allowed_transitions:
                    # No V from last run
                    if self.memory[t[0]]['V'] == None:
                        self.memory[t[0]]['V'] = self.memory[t[0]]['r']
                    # t[0] is the tail state of the edge (the next state) 
                    Vs.append(self.memory[t[0]]['V'])
#                    self.my_logger.debug("TAKEDECISION tail state: "+ t[0] +" head state: "+ 
#                                         t[1] +" V("+t[0]+") = "+ str(self.memory[t[0]]['V']))
                V[s] = self.memory[str(s)]['r'] + float(self.utils.gamma) * max(Vs)
                self.my_logger.debug("TAKEDECISION Vs: "+ str(Vs) +", max V = "+ str(max(Vs)) +" V["+str(s)+"] "+ str(V[s]))
                
        # Find the max V
        self.nextState = str(max(V.iteritems(), key=operator.itemgetter(1))[0])
        self.my_logger.debug("TAKEDECISION next state: "+ str(self.nextState))
        # Remember the V values calculated ???
        for i in V.keys():         
            self.memory[str(i)]['V'] = V[i]
            
#        vis = fuzz.visualization.VisManager.create_backend(stategraph)
#        (vis_format, data) = vis.visualize()
#        
#        with open("%s.%s" % ("states", vis_format), "wb") as fp:
#            fp.write(data)
#            fp.flush()
#            fp.close()
         
        if self.nextState !=  self.currentState:
            self.my_logger.debug( "to_next: "+str(self.nextState)+ " from_curr: "+str(self.currentState))
            
        if int(self.nextState) > int(self.currentState):
            action = "add"
        elif int(self.nextState) < int(self.currentState):
            action = "remove"
        
        self.my_logger.debug('action: ' + action)
        
        ## ACT
#        self.my_logger.debug("Taking decision with acted: " + str(self.acted))
#        if self.acted[len(self.acted)-1] == "done" :
#            ## start the action as a thread
#            self.currentState = self.nextState
#            thread.start_new_thread(self.polManager.act, (action,self.acted))
#            self.my_logger.debug("Action undertaken: " + str(action))
#        else: 
#            ## Action still takes place so do nothing
#            self.my_logger.debug("Waiting for action to finish: " +  str(action) + str(self.acted))
            
            
        self.my_logger.debug("Taking decision with acted: " + str(self.acted))
        ## Don't perform the action if we're debugging/simulating!!!
        if self.debug :
            self.my_logger.debug("TAKEDECISION sleep instead of acting...")
            time.sleep(30)
            # ...magic...
            self.currentState = str(self.nextState)
        else :
            if self.acted[len(self.acted)-1] == "done" :
                ## start the action as a thread
                thread.start_new_thread(self.polManager.act, (action, self.acted, self.currentState, self.nextState))
                self.my_logger.debug("Action undertaken: " + str(action))
                if not self.refreshMonitor.startswith("refreshed"):
                    self.VmMonitor.configure_monitoring()
                    self.refreshMonitor = "refreshed"
                self.currentState = self.nextState
            else: 
                ## Action still takes place so do nothing
                self.my_logger.debug("Waiting for action to finish: " +  str(action) + str(self.acted))
                self.refreshMonitor = "not refreshed"
        
        action = "none"
        
        return True
    
    def simulate(self):
        ## creates a sin load simulated for an hour
        for i in range(0, 3600, 30):
            latency = max(0.020, 20 * abs(math.sin(0.05 * math.radians(i))) - int(self.currentState)) 
            cpu = max(5, 60 * abs(math.sin(0.05 * math.radians(i))) - int(self.currentState))
            inlambda = max(10000, 200000 * abs(math.sin(0.05 * math.radians(i))))
            values = {'latency':latency, 'cpu':cpu, 'inlambda':inlambda}
            self.my_logger.debug( "SIMULATE state: "+str(self.currentState) +" values:"+ str(values))
            self.takeDecision(values)
            time.sleep(1)
        return
    
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
    
#    def __init__(self, policyDescription):
#        '''
#        Constructor. Requires a policy description that sets the policy. For testing purposes, we don't want to mess with the VMs. 
#        ''' 
#        self.utils = Utils.Utils()
#        self.pdesc = policyDescription
#        #self.eucacluster=eucacluster
#        #self.NoSQLCluster=NoSQLCluster
#        
#        ## Install logger
#        LOG_FILENAME = self.utils.install_dir+'/logs/Coordinator.log'
#        self.my_logger = logging.getLogger('PolicyManager')
#        self.my_logger.setLevel(logging.DEBUG)
#        
#        handler = logging.handlers.RotatingFileHandler(
#                      LOG_FILENAME, maxBytes=2*1024*1024, backupCount=5)
#        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")
#        handler.setFormatter(formatter)
#        self.my_logger.addHandler(handler)

    def act (self, action, acted, curr, next):
        self.my_logger.debug("Taking decision with acted: " + str(acted))
        if self.pdesc == "test":
            if action == "add":
                images = self.eucacluster.describe_images(self.utils.bucket_name)
                self.my_logger.debug("Found emi in db: " + str(images[0].id))
                ## Launch as many instances as are defined by the user
                num_add = int(next)-int(curr)
                self.my_logger.debug("Launching new instances: " + str(num_add))
                instances = self.eucacluster.run_instances(images[0].id, None, None, None, num_add , num_add, self.utils.instance_type)
                self.my_logger.debug("Launched new instance/instances: " + str(instances))
                acted.append("paparia")
                instances = self.eucacluster.block_until_running(instances)
                self.my_logger.debug("Running instances: " + str(instances))
                self.my_logger.debug(self.NoSQLCluster.add_nodes(instances))
                ## Make sure nodes are running for a reasonable amount of time before unblocking
                ## the add method
                time.sleep(600)
                acted.pop() 
            if action == "remove":
                acted.append("paparia")
                num_rem = int(curr)-int(next)
                for i in range(0,num_rem):
                    ## remove last node and terminate the instance
                    for hostname, host in self.NoSQLCluster.cluster.items():
                        if hostname.replace(self.NoSQLCluster.host_template, "") == str(len(self.NoSQLCluster.cluster)-1):
                            self.NoSQLCluster.remove_node(hostname)
                            if self.utils.cluster_type == "CASSANDRA":
                                time.sleep(300)
                            if self.utils.cluster_type == "HBASE":
                                time.sleep(300)
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
        elif self.pdesc == "debug":
            pass
        action = "none"

if __name__ == '__main__':
    fsm = RLFSMDecisionMaker(10)
    values = {'throughput':10000, 'added_nodes':2, 'num_nodes':10, 'latency':0.050}
    fsm.simulate()
