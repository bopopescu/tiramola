#    This file is part of TIRAMOLA.
# 
#    TIRAMOLA is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    TIRAMOLA is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with TIRAMOLA.  If not, see <http://www.gnu.org/licenses/>.

'''
Created on Oct 25, 2010

@author: christina
'''


import paramiko
import Utils
from pysqlite2 import dbapi2 as sqlite
import shutil, fileinput, time, sys

class RiakCluster(object):
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
        
        if not reconfigure:
            con = sqlite.connect(self.utils.db_file)
            cur = con.cursor()
            cur.execute('delete from clusters where cluster_id=\"'+self.utils.cluster_name+"\"")
            cur.close()
            con.close()
            self.cluster = {}
        
        print 'Cluster size: ' + str(len(self.cluster))
        for node in nodes:
            time.sleep(10)
            self.add_node(node, True)
            print self.cluster
        
        # Make /etc/hosts file for all nodes
        self.make_hosts()
        ## Save to database
        self.utils.add_to_cluster_db(self.cluster, self.cluster_id)
        
        ## Now you should be ok, so return the nodes with hostnames
        return self.cluster
    
    def start_cluster (self):
        for (clusterkey, clusternode) in self.cluster.items():
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())    
            ssh.connect(clusternode.public_dns_name, username='root', password='secretpw')    
            stdin, stdout, stderr = ssh.exec_command('/usr/sbin/riaksearch start')
            print stdout.readlines()
            sys.stdout.flush()
            
            if not clusterkey.endswith("master"):
                stdin, stdout, stderr = ssh.exec_command('/usr/sbin/riaksearch-admin join riak@' +
                                        self.cluster.get(self.host_template+"master").public_dns_name)
                print '/usr/sbin/riaksearch-admin join riak@' + self.cluster.get(self.host_template+"master").public_dns_name
                print stdout.readlines()
                sys.stdout.flush()
                    
            ssh.close()
            
    def stop_cluster (self):
        for (clusterkey, clusternode) in self.cluster.items():
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())    
            ssh.connect(clusternode.public_dns_name, username='root', password='secretpw')    
            stdin, stdout, stderr = ssh.exec_command('/usr/sbin/riaksearch stop')
            print stdout.readlines()
            ssh.close()
        
    def add_node (self, node = None, bulk = False):
        key_template_path="./templates/ssh_keys"
        
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        print node.public_dns_name
        sys.stdout.flush()
        ssh.connect(node.public_dns_name, username='root', password='secretpw')
            
        ## Check for installation dirs, otherwise exit with error message
        stderr_all = []
        stdin, stdout, stderr = ssh.exec_command('ls /usr/sbin/riaksearch')
        
        # Set hostname on the machine
        name = ""
        i = len(self.cluster) 
        if i == 0:
            name = self.host_template+"master"
        else: 
            name = self.host_template+str(i)
        
        
        stdin, stdout, stderr = ssh.exec_command('echo \"'+name+"\" > /etc/hostname")
        stdin, stdout, stderr = ssh.exec_command('hostname \"'+name+"\"")
        
#        hosts = open('/tmp/hosts', 'w')
#        hosts.write("127.0.0.1\tlocalhost\n")
#        hosts.write(node.public_dns_name + "\t" + name+"\n")
#        hosts.close()
        
        # Move files to node 
        transport = paramiko.Transport(node.public_dns_name)
        transport.connect(username = 'root', password = 'secretpw')    
        transport.open_channel("session", node.public_dns_name, "localhost")
        sftp = paramiko.SFTPClient.from_transport(transport)
            
        # Copy private and public key
        sftp.put( key_template_path+"/id_rsa","/root/.ssh/id_rsa")
        sftp.put( key_template_path+"/id_rsa.pub", "/root/.ssh/id_rsa.pub")
        sftp.put( key_template_path+"/config", "/root/.ssh/config")
        # Copy /etc/hosts
        #sftp.put( "/tmp/hosts", "/etc/hosts")
        # Copy the script that will configure riak's config files
        sftp.put( "./templates/riak/configure-riak.sh", "/root/configure-riak.sh")
        sftp.close()
        
        ## Change permissions for private key
        stdin, stdout, stderr = ssh.exec_command('chmod 0600 /root/.ssh/id_rsa')

        # Add public key to authorized_keys
        stdin, stdout, stderr = ssh.exec_command('cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys')

        ## Change mode for the script
        stdin, stdout, stderr = ssh.exec_command('chmod a+x /root/configure-riak.sh')
        
        ## Run the script for the appropriate IP (the node's)
        stdin, stdout, stderr = ssh.exec_command('./configure-riak.sh ' + node.public_dns_name)
        print stdout.readlines()
        
        ## Ping the riak node to make sure it's up
        time.sleep(15)
        stdin, stdout, stderr = ssh.exec_command('/usr/sbin/riaksearch ping')
        print stdout.readlines()
        sys.stdout.flush()
        
        # If you're not the first node of the cluster
        if(i > 0):
            # Sent request to join the riak ring (to the first node of the cluster...)
#            for (oldnodekey,oldnode) in self.cluster.items():
#                time.sleep(15)
#                if not oldnodekey.endswith("master"):
#                    oldssh = paramiko.SSHClient()
#                    oldssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#                    print oldnode.public_dns_name
#                    sys.stdout.flush()
#                    oldssh.connect(oldnode.public_dns_name, username='root', password='secretpw')
#                    stdin, stdout, stderr = oldssh.exec_command('/usr/sbin/riaksearch-admin join riak@' +
#                                        self.cluster.get(self.host_template+"master").public_dns_name)
#                    print '/usr/sbin/riaksearch-admin join riak@' + self.cluster.get(self.host_template+"master").public_dns_name
#                    print stdout.readlines()
#                    sys.stdout.flush()
#                    oldssh.close()
            
            time.sleep(15)
            stdin, stdout, stderr = ssh.exec_command('/usr/sbin/riaksearch-admin join riak@' +
                                        self.cluster.get(self.host_template+"master").public_dns_name)
            print '/usr/sbin/riaksearch-admin join riak@' + self.cluster.get(self.host_template+"master").public_dns_name
            print stdout.readlines()
            sys.stdout.flush()
        
        ssh.close()
        
        # Add node to cluster
        self.cluster[name] = node
        if not bulk:
            self.make_hosts()
        
    def remove_node (self, hostname=""):
        if(self.cluster.has_key(hostname)):
            self.cluster.pop(hostname)
        else:
            return
                
        print "New nodes:", self.cluster
        
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(self.cluster[hostname].public_dns_name, username='root', password='secretpw')
        # The node leaves the ring
        stdin, stdout, stderr = ssh.exec_command('/usr/sbin/riaksearch-admin leave')
        stdin, stdout, stderr = ssh.exec_command('/usr/sbin/riaksearch-admin ringready')
#        if len(stdout) > 0 :
        print stderr.readlines()
        # Stop the node
        stdin, stdout, stderr = ssh.exec_command('/usr/sbin/riaksearch stop')
#        if len(stdout) > 0 :
        print stderr.readlines()
        ssh.close()

    def make_hosts (self):
        hosts = open('/tmp/hosts', 'w')
        hosts.write("127.0.0.1\tlocalhost\n")

        # Write the /etc/hosts file
        for (nodekey,node) in self.cluster.items():
            hosts.write(node.public_dns_name + "\t" + nodekey+"\n")
        
        hosts.close()

        # Copy the file to all nodes 
        for (oldnodekey,oldnode) in self.cluster.items():
            transport = paramiko.Transport(oldnode.public_dns_name)
            transport.connect(username = 'root', password = 'secretpw')    
            transport.open_channel("session", oldnode.public_dns_name, "localhost")
            sftp = paramiko.SFTPClient.from_transport(transport)
            sftp.put( "/tmp/hosts", "/etc/hosts")
            sftp.close()

    def rebalance_cluster (self, threshold = 0.1):
        return True