'''
Created on Jun 23, 2011

@author: vagos
'''

import fuzz, logging, math, time, thread
import Utils

class FSMDecisionMaker():
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
        cluster_size = len(self.utils.get_cluster_from_db(self.utils.cluster_name))
        self.currentState = str(cluster_size)
        self.nextState = str(cluster_size)
        
        ## Install logger
        LOG_FILENAME = self.utils.install_dir+'/logs/Coordinator.log'
        self.my_logger = logging.getLogger('FSMDecisionMaker')
        self.my_logger.setLevel(logging.DEBUG)
        
        handler = logging.handlers.RotatingFileHandler(
                      LOG_FILENAME, maxBytes=2*1024*1024, backupCount=5)
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")
        handler.setFormatter(formatter)
        self.my_logger.addHandler(handler)
        
    
    def takeDecision(self, rcvallmetrics):
        '''
         this method reads allmetrics object created by MonitorVms and decides to change the number of participating
         virtual nodes.
        '''
        ## Take decision based on metrics
        action = "none"
        
        allmetrics = rcvallmetrics.copy()
        self.my_logger.debug( "state" + str(self.currentState))
        if not allmetrics.has_key('inlambda'):
            allmetrics['inlambda'] = 0
            
        if not allmetrics.has_key('throughput'):
            allmetrics['throughput'] = 0
            
        if not allmetrics.has_key('latency'):
            allmetrics['latency'] = 0
            
        if not allmetrics.has_key('cpu'):
            allmetrics['cpu'] = 0
        
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
        
        self.my_logger.debug( "allmetrics:" + str(allmetrics))
        
#        self.my_logger.debug( "trans" + self.utils.trans_cost)
#        self.my_logger.debug( "gain" + self.utils.gain)
        states = fuzz.fset.FuzzySet()
        ## Make all available states and connect with default weights
        for i in range(int(self.utils.initial_cluster_size), int(self.utils.max_cluster_size)+1):
            allmetrics['max_throughput'] = float(i) * float(self.utils.serv_throughput)
            allmetrics['num_nodes'] = int(i)
            states.add(fuzz.fset.FuzzyElement(str(i),eval(self.utils.gain, allmetrics)))
        
        v=[]

        for i in states.keys():
            v.append(i)
            ## Add rebalancing states
#            v.append(i+"_reb")
            
        v = set(v)
        
        stategraph = fuzz.fgraph.FuzzyGraph(viter = v, directed = True)
        
        ## Correctly connect the states (basically all transitions are possible)
        for i in states.keys():
#            self.my_logger.debug( "i" + str(i))
#            self.my_logger.debug( "start" + str(max(int(i)-int(self.utils.rem_nodes),int(self.utils.initial_cluster_size))))
#            self.my_logger.debug( "end" + str(min(int(i)+int(self.utils.add_nodes), int(self.utils.max_cluster_size))))
            for j in range(max(int(i)-int(self.utils.rem_nodes),int(self.utils.initial_cluster_size)), min(int(i)+int(self.utils.add_nodes), int(self.utils.max_cluster_size))+1):
                
                if i != str(j):
                    allmetrics['max_throughput'] = float(i) * float(self.utils.serv_throughput)
                    allmetrics['num_nodes'] = int(i)
                    allmetrics['added_nodes'] = int(i)-j
                    stategraph.connect(str(j),i,eval(self.utils.trans_cost, allmetrics))
                    
#                if i == "11":
#                    self.my_logger.debug( "11edges" + str(stategraph.edges(head=i)))
        
#        vis = fuzz.visualization.VisManager.create_backend(stategraph)
#        (vis_format, data) = vis.visualize()
#        
#        with open("%s.%s" % ("states", vis_format), "wb") as fp:
#            fp.write(data)
#            fp.flush()
#            fp.close()
         
        for transition in stategraph.edges(head=self.currentState):
#            if transition[0] == '10':
            self.my_logger.debug( "next: "+str(transition[0])+ " curr: "+str(transition[1]))
            self.my_logger.debug( "next gain: " + str(states.mu(transition[0]) * 3600))
            self.my_logger.debug( "next cost: " + str(states.mu(transition[0]) * 3600 - stategraph.mu(transition[0],transition[1]) * 500))
            self.my_logger.debug( "curr gain: " + str(states.mu(transition[1]) * 3600))
                
            if (states.mu(transition[0]) * 3600 - stategraph.mu(transition[0],transition[1])*500) > (states.mu(transition[1]) * 3600):
                if self.nextState ==  self.currentState:
                    ## if it's the first transition that works
                    self.nextState = transition[0]
                else:
                    ## if there are different competing transitions evaluate the one with the biggest gain
                    if (states.mu(transition[0]) * 3600 - stategraph.mu(transition[0],transition[1])*500) > (states.mu(self.nextState) * 3600 - stategraph.mu(self.nextState,self.currentState)*500):
                        self.nextState = transition[0]

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
        if self.acted[len(self.acted)-1] == "done" :
            ## start the action as a thread
            thread.start_new_thread(self.polManager.act, (action,self.acted,self.currentState, self.nextState))
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
            self.my_logger.debug( "state: "+str(self.currentState) +" values:"+ str(values))
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
            
        action = "none"

if __name__ == '__main__':
    fsm = FSMDecisionMaker()
    values = {'throughput':10000,'added_nodes':2,'num_nodes':10,'latency':0.050}
    fsm.simulate()
