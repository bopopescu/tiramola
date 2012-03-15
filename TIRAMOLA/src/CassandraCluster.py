'''
Created on Jun 8, 2010

@author: vagos
'''

import paramiko
import Utils
from pysqlite2 import dbapi2 as sqlite
import sys, os, shutil, fileinput

class CassandraCluster(object):
    '''
    This class holds all nodes of the db in the virtual cluster. It can start/stop individual 
    daemons as needed, thus adding/removing nodes at will. It also sets up the configuration 
    files as needed. 
    '''


    def __init__(self, initial_cluster_id="default"):
        '''
        Constructor
        '''
        ## Necessary variables
        self.cluster = {}
        self.host_template = ""
        self.cluster_id = initial_cluster_id
        self.utils = Utils.Utils()
        
        # Make sure the sqlite file exists. if not, create it and add the table we need
        con = sqlite.connect(self.utils.db_file)
        cur = con.cursor()
        try:
            clusters = cur.execute('select * from clusters',
                            ).fetchall()
            if len(clusters) > 0 :
                print """Already discovered cluster id from previous database file. Will select the defined one to work with (if it exists)."""
#                print "Found records:\n", clusters 

                clustersfromcid = cur.execute('select * from clusters where cluster_id=\"' + self.cluster_id + "\"",
                            ).fetchall()
                if len(clustersfromcid) > 0 :
                    self.cluster = self.utils.get_cluster_from_db(self.cluster_id)
    #                print self.cluster
                    for clusterkey in self.cluster.keys():
                        if not (clusterkey.find("master") == -1):
                            self.host_template = clusterkey.replace("master", "")
                    # Add self to db (eliminates existing records of same id)
                    self.utils.add_to_cluster_db(self.cluster, self.cluster_id)
                else:
                    print "No known cluster with this id - run configure before you proceed"
                     
        except sqlite.DatabaseError:
            cur.execute('create table clusters(cluster_id text, hostname text, euca_id text)')
            con.commit()
            
        cur.close()
        con.close()
        
        
#     def describe_nodes(self)

    def configure_cluster(self, nodes=None, host_template="", reconfigure=True):

        hosts = open('/tmp/hosts', 'w')
        
        i = 0
        hosts.write("127.0.0.1\tlocalhost\n")

        for node in nodes:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            print node.public_dns_name
            ssh.connect(str(node.public_dns_name), username='root', password='secretpw')
            
            ## Check for installation dirs, otherwise exit with error message
            stderr_all = []
            stdin, stdout, stderr = ssh.exec_command('ls /opt/apache-cassandra-0.7.0-beta1/')
            stderr_all.append(stderr.readlines())
            stdin, stdout, stderr = ssh.exec_command('echo "root    -       nofile  200000" >> /etc/security/limits.conf')
            stderr_all.append(stderr.readlines())
            for stderr in stderr_all:
                if len(stderr) > 0 :
                    print "ERROR - some installation files are missing"
                    return
            
            if i == 0:
                # Add the master to the /etc/hosts file
                hosts.write(node.public_dns_name + "\t" + host_template + "master\n")
                # Set hostname on the machine
                stdin, stdout, stderr = ssh.exec_command('echo \"' + host_template + "master\" > /etc/hostname")
                stdin, stdout, stderr = ssh.exec_command('hostname \"' + host_template + "master\"")
                
                # Add node to cluster
                self.cluster[host_template + "master"] = node
                
            else:
                # Make a /etc/hosts file as you go
                hosts.write(node.public_dns_name + "\t" + host_template + str(i) + "\n")
                
                # Set hostname on the machine
                stdin, stdout, stderr = ssh.exec_command('echo \"' + host_template + str(i) + "\" > /etc/hostname")
                stdin, stdout, stderr = ssh.exec_command('hostname \"' + host_template + str(i) + "\"")
                
                # Add node to cluster
                self.cluster[host_template + str(i)] = node
                
                
            ssh.close()
            
            # Save all collected known keys
            ssh.save_host_keys("/tmp/known_hosts_" + str(i))
            
            # Increase i
            i = i + 1
        
        # Decrase to have the last node in i
        i = i - 1
        
        hosts.close()
        
        key_template_path = "./templates/ssh_keys"
        
        
        
        ## Copy standard templates and name each node accordingly
        for node in nodes:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(str(node.public_dns_name), username='root', password='secretpw')
            transport = paramiko.Transport((str(node.public_dns_name), 22))
            transport.connect(username='root', password='secretpw')
            transport.open_channel("session", str(node.public_dns_name), "localhost")
            sftp = paramiko.SFTPClient.from_transport(transport)
#           Copy private and public key
            sftp.put(key_template_path + "/id_rsa", "/root/.ssh/id_rsa")
            sftp.put(key_template_path + "/id_rsa.pub", "/root/.ssh/id_rsa.pub")
            sftp.put(key_template_path + "/config", "/root/.ssh/config")
            
            ## Change permissions for private key
            stdin, stdout, stderr = ssh.exec_command('chmod 0600 /root/.ssh/id_rsa')
            
            # Add public key to authorized_keys
            stdin, stdout, stderr = ssh.exec_command('cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys')
            
            # copy necessary templates to /tmp to alter them
            shutil.copy("./templates/cassandra/cassandra.yaml", "/tmp/cassandra.yaml")
            
            cassandra_yaml = '/tmp/cassandra.yaml'
            
            # Edit cassandra.yaml to reflect seed changes
            seeds = host_template + "master"
            for j in range(1, i):
                seeds = seeds + "\n - " + host_template + str(j)
            for line in fileinput.FileInput(cassandra_yaml, inplace=1):
                line = line.replace("SEEDS", seeds).strip()
                print line        
#            os.system("sed -i '/^$/d' /tmp/cassandra.yaml")    
            # Add the node's ip to cassandra.yaml
            for line in fileinput.FileInput(cassandra_yaml, inplace=1):
                line = line.replace("LISTEN_ADDRESS", str(node.public_dns_name)).strip()
                print line
#            os.system("sed -i '/^$/d' /tmp/cassandra.yaml")
            
            if not reconfigure:
                stdin, stdout, stderr = ssh.exec_command('rm -fr /var/lib/cassandra/data/')
                
            
            # Copy files (/etc/hosts, masters, slaves and conf templates)
            sftp.put("/tmp/hosts", "/etc/hosts")
#            os.system("sed -i '/^$/d' /tmp/cassandra.yaml")
            sftp.put('/tmp/cassandra.yaml', "/opt/apache-cassandra-0.7.0-beta1/conf/cassandra.yaml")
            sftp.put('./templates/cassandra/cassandra-env.sh', "/opt/apache-cassandra-0.7.0-beta1/conf/cassandra-env.sh")
            sftp.close()
            
            ssh.close()
            
        self.host_template = host_template
        
        ## Manipulate known hosts to make a good file
        known_hosts_name = '/tmp/known_hosts'
        known_hosts = open(known_hosts_name, 'w')
        j = 0
        while j <= i:
            loop = open('/tmp/known_hosts_' + str(j), 'r')
            for fileLine in loop.readlines():
                known_hosts.write(fileLine.strip())
            loop.close()
            os.system("sed -i '/^$/d' /tmp/known_hosts")
            j = j + 1 
        known_hosts.close()
            
        for (clusterkey, clusternode) in self.cluster.items():
            for line in fileinput.FileInput(known_hosts_name, inplace=1):
                line = line.replace(str(clusternode.public_dns_name), clusterkey).strip()
                print line
        
        ## Upload perfect known hosts file
        for node in nodes:
            transport = paramiko.Transport((str(node.public_dns_name), 22))
            transport.connect(username='root', password='secretpw')
            transport.open_channel("session", str(node.public_dns_name), "localhost")
            sftp = paramiko.SFTPClient.from_transport(transport)
#            os.system("sed -i '/^$/d' /tmp/known_hosts")
            sftp.put("/tmp/known_hosts", "/root/.ssh/known_hosts")
            sftp.close()
        
        ## Save to database
        self.utils.add_to_cluster_db(self.cluster, self.cluster_id)
        
        ## Now you should be ok, so return the nodes with hostnames
        return self.cluster
                
            

    def start_cluster (self):
        for node in self.cluster.values():
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(str(node.public_dns_name), username='root', password='secretpw')
            stdin, stdout, stderr = ssh.exec_command('/opt/apache-cassandra-0.7.0-beta1/bin/cassandra')
            print stdout.readlines()
            ssh.close()
            
    def stop_cluster (self):
        for node in self.cluster.values():
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(str(node.public_dns_name), username='root', password='secretpw')
            stdin, stdout, stderr = ssh.exec_command('pkill java')
            print stdout.readlines()
            ssh.close()
            
    def add_nodes (self, new_nodes=None):
        ## Reconfigure the cluster with the last node as the provided one
        nodes = []
        nodes.append(self.cluster[self.host_template + "master"])
        for i in range(1, len(self.cluster)):
            nodes.append(self.cluster[self.host_template + str(i)])
        nodes.extend(new_nodes)
        print "New nodes:", nodes
        
        ## Stop the cluster (should ensure that new nodes are added to the ring)
#        self.stop_cluster()
        
        self.configure_cluster(nodes , self.host_template, True)
        
        ## Start the new configuration!
        self.start_cluster()
        
        ## Try to rebalance the cluster (usually rebalances the new node)
#        self.rebalance_cluster()
        
        ## Now you should be ok, so return the new node
        return nodes
        
        
    def remove_node (self, hostname=""):
        ## Remove node by hostname -- DOES NOST REMOVE THE MASTER
        nodes = []
        nodes.append(self.cluster[self.host_template + "master"])
        for i in range(1, len(self.cluster)):
            if not (self.host_template + str(i)).endswith(hostname):
                nodes.append(self.cluster[self.host_template + str(i)])
                
        print "New nodes:", nodes
        
        ## keep node
        node = self.cluster.pop(hostname)
        
        print "Removing:", hostname
        
        ## Kill all java processes on the removed node
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(str(node.public_dns_name), username='root', password='secretpw')
#        stdin, stdout, stderr = ssh.exec_command('/opt/apache-cassandra-0.7.0-beta1/bin/nodetool -h ' + hostname + ' decommission')
        stdin, stdout, stderr = ssh.exec_command('pkill -9 java')
        ssh.close()
        
        ## Reconfigure cluster
        self.configure_cluster(nodes , self.host_template, True)
        print self.cluster
        
        ## Stop the cluster (should ensure that new nodes are added to the ring)
#        self.stop_cluster()
        
        ## Start the new configuration!
        self.start_cluster()
        
        ## Now you should be ok, so return the new node
        return node
    
    def rebalance_cluster (self, threshold=0.1):
        ## /opt/apache-cassandra-0.7.0-beta1/bin/nodetool -host 62.217.120.118 getcompactionthreshold / loadbalance
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(str(self.cluster[self.host_template + "master"].public_dns_name), username='root', password='secretpw')
        stdin, stdout, stderr = ssh.exec_command('/opt/apache-cassandra-0.7.0-beta1/bin/nodetool -host ' + 
                                                  str(self.cluster[self.host_template + "master"].public_dns_name) + ' ring')
        rebalance_ip = ""
        var_min = 100000
        for line in stdout.readlines():
            if not line.startswith("Address") and not line.startswith(" "):
                splits = line.split()
                if splits[1].startswith("Up"):
                    try:
                        var = float(splits[3].replace("KB", ""))
                    except:
                        try:
                            var = 1024 * float(splits[3].replace("MB", ""))
                        except:
                            try:
                                var = 1024 * float(splits[3].replace("GB", ""))
                            except:
                                var = 1000
#                    var = float(splits[3].replace("KB","").replace("MB","").replace("GB",""))
                    if var_min > var:
                        var_min = var
                        rebalance_ip = splits[0]
                    
        print "Rebalancing node with ip: ", rebalance_ip
        stdin, stdout, stderr = ssh.exec_command('/opt/apache-cassandra-0.7.0-beta1/bin/nodetool -host ' + 
                                                  rebalance_ip + ' loadbalance')
        print stdout.readlines()
        ssh.close()
        return True
        

