# Introduction #

The DecisionMaking module is responsible for deciding which action should be performed next based on the current cluster performance.


# Details #

There are currently 3 implementations provided in the source code:
  * DecisionMaker.py. The user sets specific thresholds in Coordinator.properties file for performance metrics monitored on each cluster node such as cpu usage (load\_five), memory usage (mem\_free), etc. When the threshold for adding a node, thresholds\_add property, is exceeded for the majority of the cluster nodes the amount of nodes to add, defined in the add\_nodes property, is added. Similarly when the thresholds\_remove threshold is reached for the majority of the cluster nodes, the amount of nodes to remove, defined in the rem\_nodes property, is removed.
  * FSMDecisionMaker.py. The user sets the upper limit of nodes to be added and removed in each action (add\_nodes, rem\_nodes properties) in Coordinator.properties file. In this implementation decisions are modelled as a Markov Decision Process. Each cluster size is viewed as a state to which the cluster can transition. The gain property defines how gain is measured in each state and if the decision making module finds a state it can transition to (according to add\_nodes, rem\_nodes) with higher gain, it decides to act by adding/removing nodes accordingly. Application metrics like query throughput and latency are added in cluster monitoring and 3 different gain functions are provided in the Coordinator.properties file:
    * based on lambda, that is the query arrival rate and query latency in the cluster
    * based on cpu usage and number of nodes in the cluster
    * based on query throughput, latency and number of nodes in the cluster
  * RLFSMDecisionMaker.py. An extension of FSMDecisionMaker that uses reinforcement learning in decision making so that decisions on the most advantageous cluster size for the current load are based on previous experience. Data collected from the monitoring module are stored periodically and when called to make a decision, the system performs a clustering of measurements "similar" to the current ones, for each state. The centroid of the largest cluster (for each state) is used in calculating gains for a given state and the system will transition to the state with the highest gain. This learning feature allows the system to adapt to different input loads and to make very close to optimal decisions. There are 4 gain functions available:
    * based on throughput, latency and cluster size, this function performs best in our experiments as it trades of cluster size and cluster performance.
    * based on latency
    * based on throughput
    * based on cluster size