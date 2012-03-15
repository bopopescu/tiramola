'''
Created on Oct 30, 2010

@author: christina
'''

import paramiko
import Utils
from pysqlite2 import dbapi2 as sqlite
import sys, shutil, fileinput, time
#import easy to use xml parser called minidom:
from xml.dom.minidom import parseString

class VoldemortCluster(object):
    
    '''
    This class holds all nodes of the db in the virtual cluster. It can start/stop individual 
    daemons as needed, thus adding/removing nodes at will. It also sets up the configuration 
    files as needed. 
    '''


    def __init__(self, initial_cluster_id = "default"):
        '''
        Constructor
        '''
        ## Necessary variables
        self.cluster = {}
        self.host_template = ""
        self.cluster_id = initial_cluster_id
        self.utils = Utils.Utils()
        self.partitions = 64
        
        # Make sure the sqlite file exists. if not, create it and add the table we need
        con = sqlite.connect(self.utils.db_file)
        cur = con.cursor()
        try:
            clusters = cur.execute('select * from clusters',
                            ).fetchall()
            if len(clusters) > 0 :
                print """Already discovered cluster id from previous database file. Will select the defined one to work with (if it exists)."""
#                print "Found records:\n", clusters 

                clustersfromcid = cur.execute('select * from clusters where cluster_id=\"'+self.cluster_id+"\"",
                            ).fetchall()
                if len(clustersfromcid) > 0 :
                    self.cluster = self.utils.get_cluster_from_db(self.cluster_id)
    #                print self.cluster
                    for clusterkey in self.cluster.keys():
                        if not (clusterkey.find("master") == -1):
                            self.host_template = clusterkey.replace("master","")
                    # Add self to db (eliminates existing records of same id)
                    self.utils.add_to_cluster_db(self.cluster, self.cluster_id)
                else:
                    print "No known cluster with this id - run configure before you proceed"
                     
        except sqlite.DatabaseError:
            cur.execute('create table clusters(cluster_id text, hostname text, euca_id text)')
            con.commit()
            
        cur.close()
        con.close()
        
    def configure_cluster(self, nodes=None, host_template="", reconfigure=True):
        
        self.host_template = host_template
        
        if not reconfigure: # Clean db and cluster from old records
            con = sqlite.connect(self.utils.db_file)
            cur = con.cursor()
            cur.execute('delete from clusters where cluster_id=\"'+self.utils.cluster_name+"\"")
            cur.close()
            con.close()
            self.cluster = {}
        
            print 'Cluster size: ' + str(len(self.cluster))
    
            # Calculate the partitions to be assigned to each node (round robin fashion)
            node_partitions = {}
            for n in range(len(nodes)):
                node_partitions[n] = ""
            j=0
            while j < self.partitions:
                for k in range(len(node_partitions)):
                    if j >= self.partitions:
                        break
                    node_partitions[k] = node_partitions[k] + str(j) + ", "
                    j = j + 1
            print node_partitions

            i = 0        
            for node in nodes:
                error = self.configure_node(node, i)
                time.sleep(5)
                if error == -1:
                    return
                i = i + 1
        
            #Generate cluster.xml and sftp it to all nodes
            self.generate_cluster_xml(node_partitions, "/tmp/cluster.xml")
         
            for node in nodes:
                transport = paramiko.Transport((node.public_dns_name, 22))
                transport.connect(username = 'root', password = 'secretpw')    
                transport.open_channel("session", node.public_dns_name, "localhost")
                sftp = paramiko.SFTPClient.from_transport(transport)
                sftp.put("/tmp/cluster.xml", "/opt/voldemort-0.81/config/euca_config/config/cluster.xml")
                sftp.close()
                transport.close()
                
            # Same for /etc/hosts
            self.make_hosts()
        
            print "Configure_cluster finished loop"
            sys.stdout.flush()
            
            ## Save cluster to database
            self.utils.add_to_cluster_db(self.cluster, self.cluster_id)
        else:
            print 'Cluster size: ' + str(len(self.cluster))
            print "This cluster is already configured, use the db entries."
        
        ## Now you should be ok, so return the nodes with hostnames
        return self.cluster
                

    def start_cluster (self):
        for node in self.cluster.values():
            self.start_node(node.public_dns_name)
            
    def start_node (self, dns_name):
        print "Starting node: "+ dns_name
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(dns_name, username='root', password='secretpw')
        stdin, stdout, stderr = ssh.exec_command('/opt/voldemort-0.81/bin/voldemort-server.sh /opt/voldemort-0.81/config/euca_config  > /tmp/voldemort.log &')
        print stdout.readlines()
        sys.stdout.flush()
        ssh.close()
            
    def stop_cluster (self):
        for node in self.cluster.values():
            print "Stopping node: "+ node.public_dns_name
            self.stop_node(node.public_dns_name)
            
#            ssh = paramiko.SSHClient()
#            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#            ssh.connect(node.public_dns_name, username='root', password='secretpw')
#            stdin, stdout, stderr = ssh.exec_command('pkill java')
#            print stdout.readlines()
#            ssh.close()

    def stop_node (self, dns_name):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(dns_name, username='root', password='secretpw')
        stdin, stdout, stderr = ssh.exec_command('/opt/voldemort-0.81/bin/voldemort-stop.sh')
        print stdout.readlines()
        ssh.close()
            
    def add_node (self, node = None):
        print "Adding node "+ node.public_dns_name

        # Generate the targetCluster.xml accordingly
        current_cluster = '/tmp/current_cluster.xml'
        
        maxId = 0
        for (nodekey, mynode) in self.cluster.items():
            if not nodekey.endswith("master"):
                strId = nodekey.replace(self.host_template, '')
                if int(strId) > maxId:
                    maxId = int(strId)
        
        # The new node's Id 
        nodeId = maxId + 1
        # Configure node
        error = self.configure_node(node, nodeId)
        if error == -1:
            return
       
        # Generates the target cluster for the rebalancing and the cluster.xml 
        # file for the new node (stored in current_cluster)
        self.generate_rebalancing_cluster_xmls(current_cluster)
       
        
        # sftp the new node's cluster.xml
        transport = paramiko.Transport((node.public_dns_name, 22))
        transport.connect(username = 'root', password = 'secretpw')
        transport.open_channel("session", node.public_dns_name, "localhost")
        sftp = paramiko.SFTPClient.from_transport(transport)
        sftp.put(current_cluster, "/opt/voldemort-0.81/config/euca_config/config/cluster.xml")
        sftp.close()
        transport.close()
        
        # Update the /etc/hosts file for all
        self.make_hosts()
        
        ## Save new cluster to database
        self.utils.add_to_cluster_db(self.cluster, self.cluster_id)
        
        ## Start the new node
        self.start_node(node.public_dns_name)
        time.sleep(5)
        
        # Rebalance the cluster to get data on the new node
        # !!!! Does the decision maker call rebalance too???
        self.rebalance_cluster()
        
        ## Return the new node
        return node
    
    def add_nodes (self, nodes = None):
        for node in nodes:
            print "Adding node "+ node.public_dns_name

        # Generate the targetCluster.xml accordingly
        current_cluster = '/tmp/current_cluster.xml'
        
        maxId = 0
        for (nodekey, mynode) in self.cluster.items():
            if not nodekey.endswith("master"):
                strId = nodekey.replace(self.host_template, '')
                if int(strId) > maxId:
                    maxId = int(strId)
        
        # The new node's Id 
        #nodeId = maxId + 1
        i = maxId + 1        
        for node in nodes:
            error = self.configure_node(node, i)
            time.sleep(5)
            if error == -1:
                return
            i = i + 1
        # Configure node
        # error = self.configure_node(node, nodeId)
        #if error == -1:
        #    return
       
        # Generates the target cluster for the rebalancing and the cluster.xml 
        # file for the new node (stored in current_cluster)
        #self.generate_rebalancing_cluster_xmls(current_cluster)
        self.generate_rebalancing_multinode_cluster_xmls(current_cluster, len(nodes))
       
        
        # sftp the new nodes' cluster.xml
        for node in nodes:
            transport = paramiko.Transport((node.public_dns_name, 22))
            transport.connect(username = 'root', password = 'secretpw')
            transport.open_channel("session", node.public_dns_name, "localhost")
            sftp = paramiko.SFTPClient.from_transport(transport)
            sftp.put(current_cluster, "/opt/voldemort-0.81/config/euca_config/config/cluster.xml")
            sftp.close()
            transport.close()
        
        # Update the /etc/hosts file for all
        self.make_hosts()
        
        ## Save new cluster to database
        self.utils.add_to_cluster_db(self.cluster, self.cluster_id)
        
        ## Start the new nodes
        for node in nodes:
            self.start_node(node.public_dns_name)
        
        time.sleep(5)
        
        # Rebalance the cluster to get data on the new node
        self.rebalance_cluster(str(maxId + 1))
        
        ## Return the new node
        return nodes
    
    def remove_node (self, hostname=""):
        ## Remove node by hostname -- DOES NOT REMOVE THE MASTER
        if hostname.endswith("master"):
            print "Will NOT remove node "+ hostname
            return
        
        print "Removing node "+ self.cluster[hostname].public_dns_name
        
        remove_node = int(hostname.replace(self.host_template, ''))
        
        current_cluster = '/tmp/current_cluster.xml'
        # Generate the targetCluster.xml accordingly and send it to master for the rebalancing
        maxId = self.generate_rebalancing_cluster_xmls(current_cluster, remove_node)
        
        # !!!! Does the decision maker call rebalance too???
        self.rebalance_cluster()
        
        ## keep node
        #node = self.cluster[hostname]
        node = self.cluster.pop(hostname)
        
        print "New nodes: ", self.cluster
        
        # Update the /etc/hosts file for all
        self.make_hosts()
                
        # Stop the node to be removed
        # How do I make sure rebalancing is complete?
        self.stop_node(node.public_dns_name)
        
        ## Now you should be ok, so return the new node
        return node
    
    def rebalance_cluster (self, node_id=""):
        # bin/voldemort-rebalance.sh --url bootstrapURL --cluster targetCluster.xml
        # --parallelism maxParallelRebalancing --no-delete
        # https://github.com/voldemort/voldemort/wiki/voldemort-rebalancing
        
        # first node to de added
        first_node = self.cluster[self.host_template+node_id]
        print "rebalance_cluster: "+ first_node.public_dns_name
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(first_node.public_dns_name, username='root', password='secretpw', timeout=3000000)
    #       stdin, stdout, stderr = ssh.exec_command('/opt/voldemort-0.81/bin/voldemort-rebalance.sh --url tcp://' +
    #                                                  master.public_dns_name +':6666 --cluster targetCluster.xml '+ 
    #                                                  '--parallelism '+ str(2) +' &>> /var/log/voldemort/rebalance.log &')
    
        stdin, stdout, stderr = ssh.exec_command('/opt/voldemort-0.81/bin/voldemort-rebalance.sh --url tcp://' +
                                                      first_node.public_dns_name +':6666 --cluster targetCluster.xml '+ 
                                                     # first_node.private_dns_name +':6666 --cluster targetCluster.xml '+
                                                      '--parallelism '+ str(10) +' &>> /var/log/voldemort/rebalance.log')
        print "Sent rebalancing command: /opt/voldemort-0.81/bin/voldemort-rebalance.sh --url " 
    #        + master.public_dns_name +' --cluster targetCluster.xml '
    #        + '--parallelism '+ str(2) +' &>> /var/log/voldemort/rebalance.log &'
        ssh.close()
        return True
            
    def generate_cluster_xml (self, node_partitions, filepath):
        cluster = open(filepath, 'w')
        cluster.write("<cluster>\n\t<name>"+self.cluster_id+"</name>\n")
        
        ids = node_partitions.keys()
        ids.sort()
        print "generate_cluster_xml: "
        print ids

        for id in ids:
            if id == 0:
                node_ip = self.cluster[self.host_template+"master"].public_dns_name
                #node_ip = self.cluster[self.host_template+"master"].private_dns_name
            else:
                node_ip = self.cluster[self.host_template+str(id)].public_dns_name
                #node_ip = self.cluster[self.host_template+str(id)].private_dns_name
                
            # Make cluster.xml file as you go
            cluster.write("\t<server>\n\t\t<id>"+ str(id) +"</id>\n")
            cluster.write("\t\t<host>"+node_ip+"</host>\n")
            cluster.write("\t\t<http-port>8081</http-port>\n")
            cluster.write("\t\t<socket-port>6666</socket-port>\n")
            cluster.write("\t\t<admin-port>6667</admin-port>\n")
            cluster.write("\t\t<partitions>\n"+node_partitions[id]+"\n\t\t</partitions>\n")
            print node_partitions[id]
            cluster.write("\t</server>\n")
            
        cluster.write("</cluster>\n")
        cluster.close()

    def generate_rebalancing_cluster_xmls (self, current_cluster, remove_node=None):
        # Get the current cluster.xml file from the master node
        master = self.cluster[self.host_template+"master"]
        transport = paramiko.Transport((master.public_dns_name, 22))
        transport.connect(username = 'root', password = 'secretpw')
        transport.open_channel("session", master.public_dns_name, "localhost")
        sftp = paramiko.SFTPClient.from_transport(transport)
        sftp.get("/opt/voldemort-0.81/config/euca_config/config/cluster.xml", current_cluster)
        
        #open the xml file for reading:
        file = open(current_cluster,'r')
        #convert to string:
        data = file.read()
        #close file because we dont need it anymore:
        file.close()
        #parse the xml you got from the file
        dom = parseString(data)

        idTags = dom.getElementsByTagName('id')
        partitionsTags = dom.getElementsByTagName('partitions')

        node_partitions = {}
        i = 0
        maxId = 0
        for idTag in idTags:
            #strip off the tag (<tag>data</tag>  --->   data):
            id=idTag.toxml().replace('<id>','').replace('</id>','')
            intId = int(id)
            print "generate_rebalancing_cluster_xmls: "+ id
            if intId > maxId :
                maxId = intId
            node_partitions[intId] = partitionsTags[i].toxml().replace('<partitions>','').replace('</partitions>','').replace('<partitions/>','').lstrip().rstrip().split(", ")
            i = i + 1

        print node_partitions

        ids = node_partitions.keys()
        ids.sort()
        print ids
        if remove_node == None: # Add a node
            print "Will add a node!!!"
            leastNum = self.partitions/len(self.cluster)
            print str(leastNum) #number of partitions per node after we add a node
            nextId = maxId + 1
            node_partitions[nextId] = []
    
            old_node_partitions = {}
            for nodeId in node_partitions.keys():
                old_node_partitions[nodeId] = ', '.join(k.replace(',', '') for k in node_partitions[nodeId])
            
            print "old_node_partitions:"
            print old_node_partitions
            
            # Generate the cluster.xml file for the new node, adds a server tag with no partitions
            self.generate_cluster_xml(old_node_partitions, current_cluster)
            
            # The node id with the max number of partitions
            maxParts = 0
            # The new node will be assigned the least number of partitions for a node in this
            # particular cluster. (less data will be moved around) 
            while len(node_partitions[nextId]) < leastNum:
                for nodeId in ids:
                    # Find the node with most partitions and steal a partition from him.
                    if len(node_partitions[nodeId]) > len(node_partitions[maxParts]):
                        maxParts = nodeId
#                    print str(nodeId) +": "+ str(len(node_partitions[nodeId]))
#                if not len(node_partitions[nextId]) == leastNum:
                node_partitions[nextId].append(node_partitions[maxParts].pop())
        else: # Remove node remove_node
            print "Will remove node "+ int(remove_node) +"!!!"
            # The node id with the min number of partitions
            minParts = 0
            # Assign the node's partitions justly to all other nodes. The one with the least
            # partitions takes one at a time.
            while len(node_partitions[remove_node]) > 0:
                for nodeId in ids:
                    if len(node_partitions[nodeId]) < len(node_partitions[minParts]):
                        minParts = nodeId
#                    print str(nodeId) +": "+ str(len(node_partitions[nodeId]))
                node_partitions[minParts].append(node_partitions[remove_node].pop())
        
        #print node_partitions
            
        for nodeId in node_partitions.keys():
            node_partitions[nodeId] = ', '.join(j.replace(',', '') for j in node_partitions[nodeId])
        print "generate_rebalancing_cluster_xmls node_partitions final:"
        print node_partitions

        target_cluster = '/tmp/target_cluster.xml'

        self.generate_cluster_xml(node_partitions, target_cluster)
        # Send it to master for the rebalancing
        sftp.put(target_cluster, "/root/targetCluster.xml")
        sftp.close()
        transport.close()
        
        return maxId
    
    def generate_rebalancing_multinode_cluster_xmls (self, current_cluster, nodes_num=1, remove_node=None):
        # Get the current cluster.xml file from the master node
        master = self.cluster[self.host_template+"master"]
        transport = paramiko.Transport((master.public_dns_name, 22))
        transport.connect(username = 'root', password = 'secretpw')
        transport.open_channel("session", master.public_dns_name, "localhost")
        sftp = paramiko.SFTPClient.from_transport(transport)
        sftp.get("/opt/voldemort-0.81/config/euca_config/config/cluster.xml", current_cluster)
        
        #open the xml file for reading:
        file = open(current_cluster,'r')
        #convert to string:
        data = file.read()
        #close file because we dont need it anymore:
        file.close()
        #parse the xml you got from the file
        dom = parseString(data)

        idTags = dom.getElementsByTagName('id')
        partitionsTags = dom.getElementsByTagName('partitions')

        node_partitions = {}
        i = 0
        maxId = 0
        for idTag in idTags:
            #strip off the tag (<tag>data</tag>  --->   data):
            id=idTag.toxml().replace('<id>','').replace('</id>','')
            intId = int(id)
            print "generate_rebalancing_cluster_xmls: "+ id
            if intId > maxId :
                maxId = intId
            node_partitions[intId] = partitionsTags[i].toxml().replace('<partitions>','').replace('</partitions>','').replace('<partitions/>','').lstrip().rstrip().split(", ")
            i = i + 1

        print node_partitions

        ids = node_partitions.keys()
        ids.sort()
        print ids
        if remove_node == None: # Add nodes
            print "Will add "+ str(nodes_num) +" nodes!!!"
            leastNum = self.partitions/len(self.cluster)
            print str(leastNum) #number of partitions per node after we add the node(s)
            # ?????
            nextId = maxId + 1
            j = 0
            
            for j in range(nodes_num):
                node_partitions[nextId] = []
                nextId = nextId + 1
                j = j + 1
    
            old_node_partitions = {}
            for nodeId in node_partitions.keys():
                old_node_partitions[nodeId] = ', '.join(k.replace(',', '') for k in node_partitions[nodeId])
            
            print "old_node_partitions:"
            print old_node_partitions
            
            # Generate the cluster.xml file for the new nodes, adds a server tag with no partitions
            # for each new node
            self.generate_cluster_xml(old_node_partitions, current_cluster)
            
            # The node id with the max number of partitions
            maxParts = 0
            # The new node will be assigned the least number of partitions for a node in this
            # particular cluster. (less data will be moved around)
            nextId = maxId + 1
            j = 0
            for j in range(nodes_num):
                while len(node_partitions[nextId]) < leastNum:
                    for nodeId in ids:
                        # Find the node with most partitions and steal a partition from him.
                        if len(node_partitions[nodeId]) > len(node_partitions[maxParts]):
                            maxParts = nodeId
                        print str(nodeId) +": "+ str(len(node_partitions[nodeId]))
#                    if not len(node_partitions[nextId]) == leastNum:
                    node_partitions[nextId].append(node_partitions[maxParts].pop())
                
                j = j + 1
                nextId = nextId + 1
#                    
        else: # Remove node remove_node
            print "Will remove node "+ int(remove_node) +"!!!"
            # The node id with the min number of partitions
            minParts = 0
            # Assign the node's partitions justly to all other nodes. The one with the least
            # partitions takes one at a time.
            while len(node_partitions[remove_node]) > 0:
                for nodeId in ids:
                    if len(node_partitions[nodeId]) < len(node_partitions[minParts]):
                        minParts = nodeId
#                    print str(nodeId) +": "+ str(len(node_partitions[nodeId]))
                node_partitions[minParts].append(node_partitions[remove_node].pop())
        
        #print node_partitions
            
        for nodeId in node_partitions.keys():
            node_partitions[nodeId] = ', '.join(j.replace(',', '') for j in node_partitions[nodeId])
        print "generate_rebalancing_cluster_xmls node_partitions final:"
        print node_partitions

        target_cluster = '/tmp/target_cluster.xml'

        self.generate_cluster_xml(node_partitions, target_cluster)
        # Send it to master for the rebalancing
        sftp.put(target_cluster, "/root/targetCluster.xml")
        sftp.close()
        transport.close()
        
        return maxId
    
    def configure_node (self, node=None, id=None):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        print node.private_dns_name
        ssh.connect(node.public_dns_name, username='root', password='secretpw')
            
        ## Check for installation dirs, otherwise exit with error message
        stderr_all = []
        stdin, stdout, stderr = ssh.exec_command('ls /opt/voldemort-0.81/')
        stderr_all.append(stderr.readlines())
        for stderr in stderr_all:
            if len(stderr) > 0 :
                print "ERROR - some installation files are missing"
                return -1 # -1 error value

        strId = str(id)
        server_properties = "/tmp/server.properties"+strId
        # copy server.properties to /tmp to alter them
        shutil.copy("./templates/voldemort/server.properties", server_properties)
        print "Copied server.properties template in "+ server_properties
        # Edit the server.properties file
        for line in fileinput.FileInput(server_properties, inplace=1):
            line = line.replace("NODE_ID", strId).strip()
            print line
        
        # Set hostname on the machine
        name = ""
        if id == 0:
            name = self.host_template+"master"
        else: 
            name = self.host_template+strId

        stdin, stdout, stderr = ssh.exec_command('echo \"'+name+"\" > /etc/hostname")
        stdin, stdout, stderr = ssh.exec_command('hostname \"'+name+"\"")
        
        # Create the config and log directory for voldemort 
#        stdin, stdout, stderr = ssh.exec_command('mkdir /opt/voldemort-0.81/config/euca_config/')
        stdin, stdout, stderr = ssh.exec_command('mkdir -p /opt/voldemort-0.81/config/euca_config/config/')
        #print stdout.readlines()
        stdin, stdout, stderr = ssh.exec_command('mkdir -p /opt/voldemort-0.81/config/euca_config/data')
        stdin, stdout, stderr = ssh.exec_command('mkdir /var/log/voldemort/')
        #Raise the open file/socket limit for the machine
        #stdin, stdout, stderr = ssh.exec_command('ulimit -n 40000')
        stdin, stdout, stderr = ssh.exec_command('echo "root    -       nofile  200000" >> /etc/security/limits.conf')
            
        key_template_path="./templates/ssh_keys"
            
        # Copy private/public keys and some configuration files of voldemort 
        transport = paramiko.Transport((node.public_dns_name, 22))
        transport.connect(username = 'root', password = 'secretpw')
        transport.open_channel("session", node.public_dns_name, "localhost")
        
        sftp = paramiko.SFTPClient.from_transport(transport)
        sftp.put( key_template_path+"/id_rsa","/root/.ssh/id_rsa")
        sftp.put( key_template_path+"/id_rsa.pub", "/root/.ssh/id_rsa.pub")
        sftp.put( key_template_path+"/config", "/root/.ssh/config")
        sftp.put(server_properties, "/opt/voldemort-0.81/config/euca_config/config/server.properties")
        sftp.put("./templates/voldemort/stores.xml", "/opt/voldemort-0.81/config/euca_config/config/stores.xml")
        sftp.put("./templates/voldemort/voldemort-server.sh", "/opt/voldemort-0.81/bin/voldemort-server.sh")
        sftp.put("./templates/voldemort/voldemort-rebalance.sh", "/opt/voldemort-0.81/bin/voldemort-rebalance.sh")
        sftp.put("./templates/voldemort/log4j.properties", "/opt/voldemort-0.81/src/java/log4j.properties")
        sftp.put("./templates/voldemort/rebalancing.log4j.properties", "/opt/voldemort-0.81/config/euca_config/config/log4j.properties")
        sftp.close()
        transport.close()
            
        ## Change permissions for private key
        stdin, stdout, stderr = ssh.exec_command('chmod 0600 /root/.ssh/id_rsa')
        # Add public key to authorized_keys
        stdin, stdout, stderr = ssh.exec_command('cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys')
        ssh.close()
        
        # Add the node to the cluster
        self.cluster[name] = node
        return 0

    def make_hosts (self):
        hosts = open('/tmp/hosts', 'w')
        hosts.write("127.0.0.1\tlocalhost\n")

        # Write the /etc/hosts file
        for (nodekey,node) in self.cluster.items():
            hosts.write(node.public_dns_name + "\t" + nodekey+"\n")
        
        hosts.close()

        # Copy the file to all nodes 
        for (oldnodekey,oldnode) in self.cluster.items():
            transport = paramiko.Transport((oldnode.public_dns_name, 22))
            transport.connect(username = 'root', password = 'secretpw')    
            transport.open_channel("session", oldnode.public_dns_name, "localhost")
            sftp = paramiko.SFTPClient.from_transport(transport)
            sftp.put( "/tmp/hosts", "/etc/hosts")
            sftp.close()
            transport.close()
