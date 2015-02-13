# -*- coding: UTF-8 -*-
'''
Created on Nov 01, 2013

@author: vagoskar
'''
import Utils

import fuzz, logging, math, time, threading, random, datetime, os

import matplotlib
import deap
import numpy
# Force matplotlib to not use any Xwindows backend.
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from deap import base, creator, tools, algorithms

import pickle

from multiprocessing import Process

class GenAlgDecisionMaker(Process):
        
    def __init__(self, eucacluster=None, NoSQLCluster=None, VmMonitor=None):
        '''
        Constructor. EucaCluster is the object with which you can alter the 
        number of running virtual machines in Eucalyptus 
        NoSQLCluster contains methods to add or remove virtual machines from the virtual NoSQLCluster
        ''' 
        # initialize for simulation
        if (eucacluster == None) :
            self.polManager = None
            self.utils = Utils.Utils()
            self.acted = ["done"]
            self.runonce = "once"
            self.refreshMonitor = "refreshed"
            cluster_size = 4  # Start with 4 nodes 
            self.currentState = str(cluster_size)
            self.nextState = str(cluster_size)
            
            # keep all populations (per state) in a directory
            self.pops, self.hofs = {}, {}
            
            self.deap_init()
            
        # initialize for actual run
        else:
            self.utils = Utils.Utils()
            self.eucacluster = eucacluster
            self.NoSQLCluster = NoSQLCluster
            self.VmMonitor = VmMonitor
            self.polManager = PolicyManager("test", self.eucacluster, self.NoSQLCluster)
            self.acted = ["done"]
            self.runonce = "once"
            self.refreshMonitor = "refreshed"
            cluster_size = len(self.utils.get_cluster_from_db(self.utils.cluster_name))
            self.currentState = str(cluster_size)
            self.nextState = str(cluster_size)
            
        # # Install logger
        LOG_FILENAME = self.utils.install_dir + '/logs/Coordinator.log'
        self.my_logger = logging.getLogger('GenAlgDecisionMaker')
        self.my_logger.setLevel(logging.DEBUG)
        
        handler = logging.handlers.RotatingFileHandler(
                      LOG_FILENAME, maxBytes=2 * 1024 * 1024, backupCount=5)
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")
        handler.setFormatter(formatter)
        self.my_logger.addHandler(handler)
        
    def deap_init(self):
        creator.create("FitnessMax", base.Fitness, weights=(1.0,))
        creator.create("Individual", list, fitness=creator.FitnessMax)  # @UndefinedVariable Solve IDE variable problems due to using creator wrappers
        self.toolbox = base.Toolbox()
        
        # Attribute generator
        self.toolbox.register("attr_double", random.uniform, 0.0, 1.0)
        
        # Structure initializers
        self.toolbox.register("individual", tools.initRepeat, creator.Individual,  # @UndefinedVariable Solve IDE variable problems due to using creator wrappers
            self.toolbox.attr_double, 2)
        self.toolbox.register("population", tools.initRepeat, list, self.toolbox.individual)
        
        self.toolbox.register("evaluate", self.evalOneMax)
        self.toolbox.register("mate", tools.cxTwoPoint)
        self.toolbox.register("mutate", tools.mutGaussian, mu=1, sigma=0.3, indpb=0.25)
#             self.toolbox.register("select", tools.selTournament, tournsize=3)
        self.toolbox.register("select", tools.selBest)
        
        
    def evalOneMax(self, individual):
        
        actualServedReqs = min(self.passValues['inlambda'], self.passValues['max_throughput'])
        
        income = individual[0] * actualServedReqs  # actual request served or potentially served (reward provisioning capacity)
        
        SLA = 0
        # calculate SLA costs, anything over 2 secs evokes half the income
        if (self.passValues['latency'] > 2): 
            SLA = individual[0] * income * 1.5
        
        # stepwise calculation of power costs per server (kwh / timestep * W per server per CPU load)
        Watts = 0
        if (self.passValues['cpu'] <= 10):
            Watts = 93.7
        elif (self.passValues['cpu'] <= 20):
            Watts = 97
        elif (self.passValues['cpu'] <= 30):
            Watts = 101
        elif (self.passValues['cpu'] <= 40):
            Watts = 105
        elif (self.passValues['cpu'] <= 50):
            Watts = 110
        elif (self.passValues['cpu'] <= 60):
            Watts = 116
        elif (self.passValues['cpu'] <= 70):
            Watts = 121
        elif (self.passValues['cpu'] <= 80):
            Watts = 125
        elif (self.passValues['cpu'] <= 90):
            Watts = 129
        elif (self.passValues['cpu'] < 100):
            Watts = 133   
        else:
            Watts = 135
        
        powerCost = (0.07850 / 3600) * 30 * Watts * self.passValues['num_nodes']
        
        # Nodes cost (assume m3.xlarge VMs or similar $0.450 per Hour and calculate cost per time fraction)
        vmCost = (0.450 / 3600) * 30 * self.passValues['num_nodes']
        
        # costs calculation
        costs = SLA + powerCost + vmCost
        
        # reward how close it is to the user function     
        gainUser = eval(self.utils.gain, self.passValues) 
        
        # try to balance income and costs
        rew = 1 / max (0.0000001, abs(income - costs))
        
        return rew, income, costs, gainUser
    
    def evalUser(self, individual):
        
        # Use the individual modifiers for the gain function
        gainAlg = individual[0] * self.passValues['latency'] + individual[1] * self.passValues['cpu'] + individual[2] * self.passValues['inlambda'] 
        + individual[3] * self.passValues['max_throughput'] + individual[4] * self.passValues['num_nodes']
        
        # reward how close it is to the user function     
        gainUser = eval(self.utils.gain, self.passValues)   
        
        rew = 1 / max(0.001, (abs(gainAlg - gainUser) / gainUser))  # real fast convergence, do not divide by 0
        return (rew, gainAlg, gainUser)
    
    def evalMultiVar(self, individual):
        
        actualServedReqs = min(self.passValues['inlambda'], self.passValues['max_throughput'])
        
        income = individual[3] * actualServedReqs  # actual request served or potentially served (reward provisioning capacity)
        
        # calculate SLA costs, anything over 2 secs evokes half the income
        if (self.passValues['latency'] > 2): 
            SLA = 0.5 * actualServedReqs
        else: 
            SLA = 0
        
        # costs calculation
        costs = individual[0] * SLA + individual[1] * 10 * self.passValues['cpu'] + individual[2] * 100 * self.passValues['num_nodes'] 
        
        # reward how close it is to the user function     
        gainUser = eval(self.utils.gain, self.passValues) 
        
        return income, costs, gainUser
    
    def aggregateMetrics(self, rcvallmetrics):    
        allmetrics = rcvallmetrics.copy()
        self.my_logger.debug("state" + str(self.currentState))
        if 'inlambda' not in allmetrics:
            allmetrics['inlambda'] = 0
            
        if 'throughput' not in allmetrics :
            allmetrics['throughput'] = 0
            
        if 'qlen' not in allmetrics:
            allmetrics['qlen'] = 0
            
        if 'latency' not in allmetrics:
            allmetrics['latency'] = 0
            
        if 'cpu' not in allmetrics:
            allmetrics['cpu'] = 0
        
        # # Aggreggation of YCSB client metrics
        clients = 0
        nodes = 0
        for host in list(allmetrics.values()):
            if isinstance(host, dict):
                # # YCSB aggregation
                if "ycsb_LAMDA_1" in host:
                    for key in list(host.keys()):
                        if key.startswith('ycsb_LAMDA'):
                            allmetrics['inlambda'] += float(host[key])
                        if key.startswith('ycsb_THROUGHPUT'):
                            allmetrics['throughput'] += float(host[key])
                        if key.startswith('ycsb_READ') or key.startswith('ycsb_UPDATE') or key.startswith('ycsb_RMW') or key.startswith('ycsb_INSERT'):
                            allmetrics['latency'] += float(host[key])
                            if host[key] > 0:
                                clients += 1
                # # H2RDF aggregation
                if "in_THROUGHPUT" in host:
                    for key in list(host.keys()):
                        if key.startswith('in_real'):
                            allmetrics['inlambda'] += float(host[key])
                        if key.startswith('out_real'):
                            allmetrics['throughput'] += float(host[key])
                        if key.startswith('qlen'):
                            allmetrics['qlen'] += float(host[key])
                # # CPU aggregation
                for key in list(host.keys()):
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
            
        return allmetrics
        
    def visualize(self, stategraph):
        vis = fuzz.visualization.VisManager.create_backend(stategraph, plugin="graph_pydot")
        (vis_format, data) = vis.visualize()
        
        with open("%s.%s" % ("mystates", vis_format), "wb") as fp:
            fp.write(data)
            fp.flush()
            fp.close()
    
    def act(self, action):
        self.my_logger.debug('action: ' + action)
            
        self.my_logger.debug("Taking decision with acted: " + str(self.acted))
        if self.acted[len(self.acted) - 1] == "done" :
            # Check if we are not in simulation mode
            if (self.polManager != None) :
                    # start the action as a thread
                    threading.Thread(target=self.polManager.act, args=(action, self.acted, self.currentState, self.nextState)).start()
            self.my_logger.debug("Action undertaken: " + str(action))
            if not self.refreshMonitor.startswith("refreshed"):
                self.VmMonitor.configure_monitoring()
                self.refreshMonitor = "refreshed"
            self.currentState = self.nextState
        else: 
            # # Action still takes place so do nothing
            self.my_logger.debug("Waiting for action to finish: " + str(action) + str(self.acted))
            self.refreshMonitor = "not refreshed"
            
        return 'none'
    
    def takeDecisionNew(self, rcvallmetrics):
        '''
         this method reads allmetrics object created by MonitorVms and decides to change the number of participating
         virtual nodes.
        '''
        action = "none"
        allmetrics = self.aggregateMetrics(rcvallmetrics)
        
        states = fuzz.fset.FuzzySet()
        # # Make all available states and connect with default weights
        for i in range(int(self.utils.initial_cluster_size), int(self.utils.max_cluster_size) + 1):
            allmetrics['max_throughput'] = float(i) * float(self.utils.serv_throughput)
            allmetrics['num_nodes'] = int(i)
            if (i not in self.pops.keys()) or len(self.pops[i]) < 100:
                self.pops[i] = self.toolbox.population(n=300)
            if (i not in self.hofs.keys()) or len(self.hofs[i]) < 1:
                self.hofs[i] = tools.HallOfFame(1)
                self.hofs[i].update(self.pops[i])  # there should be at least a random individual
                self.my_logger.debug("Updating HoF for: " + str(i))
            
#             self.my_logger.debug("adding state: " + str(i) )
#             self.my_logger.debug("individual: " + str(self.hofs[i]) )

            states.add(fuzz.fset.FuzzyElement(str(i), self.evalOneMax(self.hofs[i][0])[0]))
        
        v = []

        for i in list(states.keys()):
            v.append(i)
            
        v = set(v)
        
        stategraph = fuzz.fgraph.FuzzyGraph(viter=v, directed=True)
        
        # # Correctly connect the states (basically all transitions are possible)
        for i in list(states.keys()):
            for j in range(max(int(i) - int(self.utils.rem_nodes), int(self.utils.initial_cluster_size)),
                           min(int(i) + int(self.utils.add_nodes), int(self.utils.max_cluster_size)) + 1):
                
                if i != str(j):
                    allmetrics['max_throughput'] = float(i) * float(self.utils.serv_throughput)
                    allmetrics['num_nodes'] = int(i)
                    allmetrics['added_nodes'] = int(i) - j
                    # Vertices are weighted using the trans cost as defined by the user
                    stategraph.connect(str(j), i, eval(self.utils.trans_cost, allmetrics))

# Visualize the state graph
#         self.visualize(stategraph)
         
        for transition in stategraph.edges(head=self.currentState):
            self.my_logger.debug("next: " + str(transition[0]) + " curr: " + str(transition[1]))
            self.my_logger.debug("next gain: " + str(states.mu(transition[0])))
            self.my_logger.debug("next cost: " + str(states.mu(transition[0]) - stategraph.mu(transition[0], transition[1])))
            self.my_logger.debug("curr gain: " + str(states.mu(transition[1])))
                
            if (states.mu(transition[0]) - stategraph.mu(transition[0], transition[1])) > states.mu(transition[1]):
                if self.nextState == self.currentState:
                    # # if it's the first transition that works
                    self.nextState = transition[0]
                else:
                    # # if there are different competing transitions evaluate the one with the biggest gain
                    if (states.mu(transition[0]) - stategraph.mu(transition[0], transition[1])) > (states.mu(self.nextState) - stategraph.mu(self.nextState, self.currentState)):
                        self.nextState = transition[0]

        if self.nextState != self.currentState:
            self.my_logger.debug("to_next: " + str(self.nextState) + " from_curr: " + str(self.currentState))
            
        if int(self.nextState) > int(self.currentState):
            action = "add"
        elif int(self.nextState) < int(self.currentState):
            action = "remove"
        
        # Start a new thread to act
        action = self.act(action)
        
        return True
    
    def simEA(self):
        random.seed(random.randint(0, 1000))

        self.paretoFront = tools.ParetoFront()
        self.crossoverProb = 0.3
        self.mutationProb = 0.7
        self.ngen = 100  # 100 fast (8mins), 200 good enough (15 mins), 500 for actual run (40 mins runtime)
        self.Mu = 100
        self.Lambda = 300
        
        stats = tools.Statistics(lambda ind: ind.fitness.values)
        stats.register("avg", numpy.mean)
        stats.register("std", numpy.std)
        stats.register("min", min)
        stats.register("max", max)
        
        # creates a sin load simulated for an hour
        x, platency, pcpu, pinlambda, pstates, pthrough, pgainUser, pcosts, pincome, prew = [], [], [], [], [], [], [], [], [], []
        
        # simulate the sinusoid input 20 times
        for i in range(0, 20):
            for i in range(0, 3600, 30):
                latency = max(0.020, 20 * abs(math.sin(0.05 * math.radians(i))) - int(self.currentState))
                cpu = max(5, 60 * abs(math.sin(0.05 * math.radians(i))) - int(self.currentState))
                inlambda = max(10000, 200000 * abs(math.sin(0.05 * math.radians(i))))
                num_nodes = int(self.currentState)
                max_throughput = float(num_nodes) * float(self.utils.serv_throughput)
                
                for j in range(max(int(num_nodes) - int(self.utils.rem_nodes), int(self.utils.initial_cluster_size)),
                               min(int(num_nodes) + int(self.utils.add_nodes), int(self.utils.max_cluster_size)) + 1):
                    if j not in self.pops.keys():
                        self.pops[j] = self.toolbox.population(n=300)
                    if j not in self.hofs.keys():
                        self.hofs[j] = tools.HallOfFame(1)
                        self.hofs[j].update(self.pops[j])  # there should be at least a random individual
                        self.my_logger.debug(str(j) + " individual: " + str(self.hofs[j]))
                
                self.passValues = {'latency':latency, 'cpu':cpu, 'inlambda':inlambda, 'max_throughput':max_throughput, 'num_nodes':num_nodes} 
                self.my_logger.debug("state: " + str(self.currentState) + " values:" + str(self.passValues))
                
                # Advance all states
                jobs = []
                for j in range(max(int(num_nodes) - int(self.utils.rem_nodes), int(self.utils.initial_cluster_size)),
                               min(int(num_nodes) + int(self.utils.add_nodes), int(self.utils.max_cluster_size)) + 1):
                    self.my_logger.debug("advancing EA for: state " + str(j))
                    p = Process(target=algorithms.eaMuCommaLambda , args=(self.pops[j], self.toolbox, self.Mu, self.Lambda, self.crossoverProb, self.mutationProb, self.ngen, stats, self.hofs[j], False))
                    jobs.append(p)
                    p.start()
    
                for p in jobs:
                    p.join()
                    
    #                 algorithms.eaMuCommaLambda(self.pops[j], self.toolbox, self.Mu, self.Lambda,
    #                                         cxpb=self.crossoverProb, mutpb=self.mutationProb, ngen=self.ngen,
    #                                         stats=stats, halloffame=self.hofs[j], verbose=False)
    
                # take actual decision with user gain function
                self.takeDecisionNew(self.passValues)
                
                x.append(i) 
                platency.append(latency)
                pcpu.append(cpu)
                pinlambda.append(inlambda)
                pstates.append(self.currentState)
                pthrough.append(max_throughput)
                rew, income, costs, gainUser = self.evalOneMax(self.hofs[num_nodes][0])
                pincome.append(income)
                pcosts.append(costs)
                pgainUser.append(gainUser)
                prew.append(rew)
                
                # print + erase
    #             print(str(self.hofs[num_nodes]))
                self.hofs[num_nodes].clear()
        
        plt.figure(figsize=(15, 10))
        plt.subplot(4, 2, 1)    
        plt.xlabel('Time')
        plt.ylabel('lambda (req/s) as measured client side')
        plt.title('lambda variation')
        plt.plot(x, pinlambda, "-b")
         
        plt.subplot(4, 2, 2)
        plt.xlabel('Time')
        plt.ylabel('latency (s)')
        plt.title('Average client latency')
        plt.plot(x, platency, "-r")
         
        plt.subplot(4, 2, 3)
        plt.xlabel('Time')
        plt.ylabel('Avg CPU load (%)')
        plt.title('Average cluster node CPU load')
        plt.plot(x, pcpu, "-g")
         
         
        plt.subplot(4, 2, 4)
        plt.xlabel('Time')
        plt.ylabel('Number of nodes (#)')
        plt.title('Nodes comprising the cluster - State')
        plt.plot(x, pstates, "-r")
        
        plt.subplot(4, 2, 5)
        plt.xlabel('Time')
        plt.ylabel('Max Throughput')
        plt.title('Max Throughput')
        plt.plot(x, pthrough, "-g")
        
        plt.subplot(4, 2, 6)
        plt.xlabel('Time')
        plt.ylabel('Gain')
        plt.title('Gain')
        plt.plot(x, pgainUser, "-g")
        
        plt.subplot(4, 2, 7)
        plt.xlabel('Time')
        plt.ylabel('Income & Costs')
        plt.title('Income & Costs')
        plt.plot(x, pincome, "-b")
        plt.plot(x, pcosts, "-r")
        
        plt.subplot(4, 2, 8)
        plt.xlabel('Time')
        plt.ylabel('Reward')
        plt.title('Reward')
        plt.plot(x, prew, "-b")
         
        plt.savefig(os.path.expanduser('~/tiramola_png/lambda_') + datetime.datetime.now().strftime("%Y-%m-%d_%H:%M") + '.png', dpi=600)
#         plt.show()

            

#         for i in range(7200, 2 * 7200, 30):
#             latency = max(0.020, 20 * abs(math.sin(0.05 * math.radians(i))) - int(self.currentState))
#             cpu = max(5, 50 * abs(math.sin(0.05 * math.radians(i))) - int(self.currentState))
#             inlambda = max(10000, 100000 * abs(math.sin(0.05 * math.radians(i))))
#             max_throughput = float(self.currentState) * float(self.utils.serv_throughput)
#             num_nodes = int(self.currentState)
#             self.passValues = {'latency':latency, 'cpu':cpu, #         self.toolbox.register("map", self.pool.map)'inlambda':inlambda, 'max_throughput':max_throughput, 'num_nodes':num_nodes} 
#             self.my_logger.debug("state: " + str(self.currentState) + " values:" + str(values))
#             algorithms.eaMuCommaLambda(self.pop, self.toolbox, self.Mu, self.Lambda, cxpb=self.crossoverProb, mutpb=self.mutationProb, ngen=self.ngen,
#                                 stats=stats, halloffame=self.hof, verbose=True)
#             print (self.hof)
#             self.takeDecisionOld(self.passValues)
#             x.append(i)
#             platency.append(latency)
#             pcpu.append(cpu)
#             pinlambda.append(inlambda)
#             pstates.append(self.currentState)
    

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
        self.eucacluster = eucacluster
        self.NoSQLCluster = NoSQLCluster
        
        # # Install logger
        LOG_FILENAME = self.utils.install_dir + '/logs/Coordinator.log'
        self.my_logger = logging.getLogger('PolicyManager')
        self.my_logger.setLevel(logging.DEBUG)
        
        handler = logging.handlers.RotatingFileHandler(
                      LOG_FILENAME, maxBytes=2 * 1024 * 1024, backupCount=5)
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")
        handler.setFormatter(formatter)
        self.my_logger.addHandler(handler)

    def act (self, action, acted, curr, next_st):
        self.my_logger.debug("Taking decision with acted: " + str(acted))
        if self.pdesc == "test":
            if action == "add":
                images = self.eucacluster.describe_images(self.utils.bucket_name)
                self.my_logger.debug("Found emi in db: " + str(images[0].id))
                # # Launch as many instances as are defined by the user
                num_add = int(next_st) - int(curr)
                self.my_logger.debug("Launching new instances: " + str(num_add))
                instances = self.eucacluster.run_instances(images[0].id, None, None, None, num_add , num_add, self.utils.instance_type)
                self.my_logger.debug("Launched new instance/instances: " + str(instances))
                acted.append("paparia")
                instances = self.eucacluster.block_until_running(instances)
                self.my_logger.debug("Running instances: " + str(instances))
                self.my_logger.debug(self.NoSQLCluster.add_nodes(instances))
                # # Make sure nodes are running for a reasonable amount of time before unblocking
                # # the add method
                time.sleep(600)
                acted.pop() 
            if action == "remove":
                acted.append("paparia")
                num_rem = int(curr) - int(next_st)
                for _ in range(0, num_rem):
                    # # remove last node and terminate the instance
                    for hostname, host in list(self.NoSQLCluster.cluster.items()):
                        if hostname.replace(self.NoSQLCluster.host_template, "") == str(len(self.NoSQLCluster.cluster) - 1):
                            self.NoSQLCluster.remove_node(hostname)
                            if self.utils.cluster_type == "CASSANDRA":
                                time.sleep(300)
                            if self.utils.cluster_type == "HBASE":
                                time.sleep(300)
                            if self.utils.cluster_type == "HBASE92":
                                time.sleep(300)
                            self.eucacluster.terminate_instances([host.id])
                            break
                    
                # # On reset to original cluster size, restart the servers
#                if (len(self.NoSQLCluster.cluster) == int(self.utils.initial_cluster_size)):
#                    self.NoSQLCluster.stop_cluster()
#                    self.NoSQLCluster.start_cluster()
                    
                acted.pop() 
#            if not action == "none":
#                self.my_logger.debug("Starting rebalancing for active cluster.")
#                self.NoSQLCluster.rebalance_cluster()
            
        action = "none"
        
if __name__ == '__main__':
    GA = GenAlgDecisionMaker()
    values = {'throughput':10000, 'added_nodes':2, 'num_nodes':4, 'latency':0.050}
    try:
        GA.pops = pickle.load(open('pops.p', 'rb'))
#         GA.hofs = pickle.load(open('hofs.p', 'rb'))
    except:
        GA.pops = {}
        GA.hofs = {}
    print ("loaded " + str(len(GA.pops)) + " populations")

    GA.simEA()
    

    pickle.dump(GA.pops, open('pops.p', 'wb'))
#     pickle.dump(GA.hofs, open('hofs.p', 'wb'))
    print ("stored " + str(len(GA.pops)) + " populations and " + str(len(GA.hofs)) + " HOFs.")
        


    
