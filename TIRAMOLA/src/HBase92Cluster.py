'''
Created on Jun 8, 2010

@author: vagos
'''

import paramiko
import Utils
from pysqlite2 import dbapi2 as sqlite
import pexpect, os, shutil, fileinput, sys, logging

class HBase92Cluster(object):
    '''
    This class holds all nodes of the db in the virtual cluster. It can start/stop individual 
    daemons as needed, thus adding/removing nodes at will. It also sets up the configuration 
    files as needed. 
    '''


    def __init__(self, initial_cluster_id = "default"):
        '''
        Constructo
        '''
        ## Necessary variables
        self.cluster = {}
        self.host_template = ""
        self.cluster_id = initial_cluster_id
        self.utils = Utils.Utils()
        self.quorum = ""
        
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
        
        ## Install logger
        LOG_FILENAME = self.utils.install_dir+'/logs/Coordinator.log'
        self.my_logger = logging.getLogger('HBaseCluster')
        self.my_logger.setLevel(logging.DEBUG)
        
        handler = logging.handlers.RotatingFileHandler(
                      LOG_FILENAME, maxBytes=2*1024*1024, backupCount=5)
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")
        handler.setFormatter(formatter)
        self.my_logger.addHandler(handler)
        
        
#     def describe_nodes(self)

    def configure_cluster(self, nodes=None, host_template="", reconfigure=True):
        
        ## Check installation and print errors for nodes that do not exist/
        ## can not connect/have incorrect installed versions and/or paths 

        hosts = open('/tmp/hosts', 'w')
        masters = open('/tmp/masters', 'w')
        slaves = open('/tmp/slaves', 'w')
        
        # copy necessary templates to /tmp to alter them
        shutil.copy("./templates/hadoop101/core-site.xml", "/tmp/core-site.xml")
        shutil.copy("./templates/hadoop101/mapred-site.xml", "/tmp/mapred-site.xml")
        shutil.copy("./templates/hadoop101/hdfs-site.xml", "/tmp/hdfs-site.xml")
        shutil.copy("./templates/hbase92/hbase-site.xml", "/tmp/hbase-site.xml")
        shutil.copy("./templates/hbase92/hbase-env.sh","/tmp/hbase-env.sh")
        shutil.copy("./templates/hbase92/hadoop-env.sh","/tmp/hadoop-env.sh")
        
#        core_site = open('/tmp/core-site.xml', 'rw')
#        mapred_site = open('/tmp/mapred-site.xml', 'rw')
#        hbase_site = open('/tmp/hbase-site.xml', 'rw')
        
        core_site = '/tmp/core-site.xml'
        mapred_site = '/tmp/mapred-site.xml'
        hbase_site = '/tmp/hbase-site.xml'
        
        i = 0
        hosts.write("127.0.0.1\tlocalhost\n")

        for node in nodes:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.my_logger.debug("Starting config for node: " + node.public_dns_name) 
            ssh.connect(node.public_dns_name, username='root', password='secretpw')
            
            ## Check for installation dirs, otherwise exit with error message
            stderr_all = []
            stdin, stdout, stderr = ssh.exec_command('ls /opt/hadoop-1.0.1/')
            stderr_all.append(stderr.readlines())
            stdin, stdout, stderr = ssh.exec_command('ls /opt/hbase-0.92.0/')
            stderr_all.append(stderr.readlines())
            stdin, stdout, stderr = ssh.exec_command('echo "root    -       nofile  200000" >> /etc/security/limits.conf')
            stderr_all.append(stderr.readlines())
            stdin, stdout, stderr = ssh.exec_command('swapoff -a -v')
            for stderr in stderr_all:
                if len(stderr) > 0 :
                    self.my_logger.debug("ERROR - some installation files are missing")
                    return
            
            if i==0:
                # Add the master to the /etc/hosts file
                hosts.write(node.private_dns_name + "\t" + host_template+"master\n")
                # Add the master to the masters file
                masters.write(host_template+"master\n")
                # Set hostname on the machine
                stdin, stdout, stderr = ssh.exec_command('echo \"'+host_template+"master\" > /etc/hostname")
                stdin, stdout, stderr = ssh.exec_command('hostname \"'+host_template+"master\"")
                
                for line in fileinput.FileInput(core_site,inplace=1):
                    line = line.replace("NAMENODE_IP",host_template+"master").strip()
                    print line
                for line in fileinput.FileInput(hbase_site,inplace=1):
                    line = line.replace("NAMENODE_IP",host_template+"master").strip()
                    print line
                for line in fileinput.FileInput(mapred_site,inplace=1):
                    line = line.replace("JOBTRACKER_IP",host_template+"master").strip()
                    print line
                
                ## create namenode/datanode dirs
                stdin, stdout, stderr = ssh.exec_command('mkdir /opt/hdfsnames/')
                
                # Add node to cluster
                self.cluster[host_template+"master"] = node
                
            else:
                # Make a /etc/hosts file as you go
                hosts.write(node.private_dns_name + "\t" + host_template + str(i) +"\n")
                
                # Add all to the slaves file
                slaves.write(host_template+ str(i)+"\n")
                
                # Set hostname on the machine
                stdin, stdout, stderr = ssh.exec_command('echo \"'+host_template+str(i)+"\" > /etc/hostname")
                stdin, stdout, stderr = ssh.exec_command('hostname \"'+host_template+str(i)+"\"")
                
                ## create namenode/datanode dirs
                stdin, stdout, stderr = ssh.exec_command('mkdir /opt/hdfsdata/')
                
                # Add node to cluster
                self.cluster[host_template+ str(i)] = node
                
                
            ssh.close()

            
            # Save all collected known keys
            ssh.save_host_keys("/tmp/known_hosts_"+str(i))
            
            # Increase i
            i = i+1
        
        # Decrase to have the last node in i
        i = i-1
        
        # Add the last node to the masters file (secondary namenode)
        masters.write(host_template+ str(i)+"\n")
        
        ## make the quorum
        if self.quorum=="":
#            self.quorum = host_template+"master,"+host_template+str((int(self.utils.initial_cluster_size)/2))+","+host_template+str(int(self.utils.initial_cluster_size)-1)
            self.quorum = host_template+"master"
        for line in fileinput.FileInput(hbase_site,inplace=1):
            line = line.replace("ZK_QUORUM_IPs", self.quorum ).strip()
            print line
            
        hosts.close()
        masters.close()
        slaves.close()
        
        key_template_path="./templates/ssh_keys"
        
        
        
        ## Copy standard templates and name each node accordingly
        for node in nodes:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(node.public_dns_name, username='root', password='secretpw')
            
            ## Enlarge the user limit on open file descriptors 
            ## (workaround for HDFS-127:http://wiki.apache.org/hadoop/Hbase/Troubleshooting#A7) 
            stdin, stdout, stderr = ssh.exec_command('ulimit -HSn 32768')
            
            ## Sync clocks over IPv6
            stdin, stdout, stderr = ssh.exec_command('ntpdate 2.pool.ntp.org')
            
            transport = paramiko.Transport((node.public_dns_name, 22))
            transport.connect(username = 'root', password = 'secretpw')
            transport.open_channel("session", node.public_dns_name, "localhost")
            sftp = paramiko.SFTPClient.from_transport(transport)
#           Copy private and public key
            sftp.put( key_template_path+"/id_rsa","/root/.ssh/id_rsa")
            sftp.put( key_template_path+"/id_rsa.pub", "/root/.ssh/id_rsa.pub")
            sftp.put( key_template_path+"/config", "/root/.ssh/config")
            
            ## Change permissions for private key
            stdin, stdout, stderr = ssh.exec_command('chmod 0600 /root/.ssh/id_rsa')
            
            # Add public key to authorized_keys
            stdin, stdout, stderr = ssh.exec_command('cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys')
#            print stdout.readlines()
            
            # Copy files (/etc/hosts, masters, slaves and conf templates) removing empty lines
            sftp.put( "/tmp/hosts", "/etc/hosts")
            os.system("sed -i '/^$/d' /tmp/core-site.xml")
            sftp.put( "/tmp/core-site.xml","/opt/hadoop-1.0.1/conf/core-site.xml")
            os.system("sed -i '/^$/d' /tmp/mapred-site.xml")
            sftp.put( "/tmp/mapred-site.xml","/opt/hadoop-1.0.1/conf/mapred-site.xml")
            os.system("sed -i '/^$/d' /tmp/hdfs-site.xml")
            sftp.put( "/tmp/hdfs-site.xml","/opt/hadoop-1.0.1/conf/hdfs-site.xml")
            sftp.put( "/tmp/masters", "/opt/hadoop-1.0.1/conf/masters")
            sftp.put( "/tmp/slaves", "/opt/hadoop-1.0.1/conf/slaves")
            os.system("sed -i '/^$/d' /tmp/hbase-site.xml")
            sftp.put( "/tmp/hbase-site.xml", "/opt/hbase-0.92.0/conf/hbase-site.xml")
            sftp.put( "/tmp/hbase-site.xml", "/opt/hadoop-1.0.1/conf/hbase-site.xml")
            sftp.put( "/tmp/slaves", "/opt/hbase-0.92.0/conf/regionservers")
            sftp.put( "/tmp/hbase-env.sh", "/opt/hbase-0.92.0/conf/hbase-env.sh")
            sftp.put( "/tmp/hadoop-env.sh", "/opt/hadoop-1.0.1/conf/hadoop-env.sh")
            sftp.close()
            
            ssh.close()
            
        self.host_template = host_template
        
        ## Manipulate known hosts to make a good file
        known_hosts_name = '/tmp/known_hosts'
        known_hosts = open(known_hosts_name, 'w')
        j = 0
        while j <= i:
            loop = open('/tmp/known_hosts_'+str(j), 'r')
            for fileLine in loop.readlines():
                known_hosts.write(fileLine.strip())
            loop.close()
            os.system("sed -i '/^$/d' /tmp/known_hosts")
            j = j + 1 
        known_hosts.close()
            
        for (clusterkey, clusternode) in self.cluster.items():
            for line in fileinput.FileInput(known_hosts_name,inplace=1):
                line = line.replace(clusternode.public_dns_name, clusterkey).strip()
                print line
#            print clusterkey, clusternode.public_dns_name
        
        
        ## Upload perfect file
        for node in nodes:
            transport = paramiko.Transport((node.public_dns_name, 22))
            transport.connect(username = 'root', password = 'secretpw')
            transport.open_channel("session", node.public_dns_name, "localhost")
            sftp = paramiko.SFTPClient.from_transport(transport)
            os.system("sed -i '/^$/d' /tmp/known_hosts")
            sftp.put( "/tmp/known_hosts", "/root/.ssh/known_hosts")
            sftp.close()
        
        ## Format namenode on the master
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(self.cluster[host_template+"master"].public_dns_name, username='root', password='secretpw')
        if not reconfigure:
            ## format the namenode (all previous data will be lost!!!
            stdin, stdout, stderr = ssh.exec_command('echo "Y" | /opt/hadoop-1.0.1/bin/hadoop namenode -format')
            self.my_logger.debug("Namenode formatted:" + str(stderr.readlines()))
        ssh.close()
        
        ## Save to database
        self.utils.add_to_cluster_db(self.cluster, self.cluster_id)
        
        ## Now you should be ok, so return the nodes with hostnames
        return self.cluster
                
            

    def start_cluster (self):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#        print self.host_template+"master"
#        print self.cluster[self.host_template+"master"].public_dns_name
        ssh.connect(self.cluster[self.host_template+"master"].public_dns_name, username='root', password='secretpw')
        stdin, stdout, stderr = ssh.exec_command('/opt/hadoop-1.0.1/bin/start-dfs.sh')
        self.my_logger.debug(str(stdout.readlines()))
        stdin, stdout, stderr = ssh.exec_command('/opt/hadoop-1.0.1/bin/start-mapred.sh')
        self.my_logger.debug(str(stdout.readlines()))
        stdin, stdout, stderr = ssh.exec_command('/opt/hbase-0.92.0/bin/start-hbase.sh')
        self.my_logger.debug(str(stdout.readlines()))
        ssh.close()
            
    def stop_cluster (self):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#        print self.host_template+"master"
#        print self.cluster[self.host_template+"master"].public_dns_name
        ssh.connect(self.cluster[self.host_template+"master"].public_dns_name, username='root', password='secretpw')
        stdin, stdout, stderr = ssh.exec_command('/opt/hbase-0.92.0/bin/stop-hbase.sh')
        self.my_logger.debug(str(stdout.readlines()))
        stdin, stdout, stderr = ssh.exec_command('/opt/hadoop-1.0.1/bin/stop-dfs.sh')
        self.my_logger.debug(str(stdout.readlines()))
        stdin, stdout, stderr = ssh.exec_command('/opt/hadoop-1.0.1/bin/stop-mapred.sh')
        self.my_logger.debug(str( stdout.readlines()))
        ssh.close()
        
    def add_nodes (self, new_nodes = None):
        ## Reconfigure the cluster with the last node as the provided one
        nodes = []
        nodes.append(self.cluster[self.host_template+"master"])
        for i in range(1,len(self.cluster)):
            nodes.append(self.cluster[self.host_template+str(i)])
        nodes.extend(new_nodes)
        self.my_logger.debug("New nodes:"+str(nodes))
        
        self.configure_cluster( nodes , self.host_template, True)
        
        ## Start the new configuration!
        self.start_cluster()
        
        ## Try to rebalance the cluster (usually rebalances the new node)
#        self.rebalance_cluster()
        
        ## Now you should be ok, so return the new node
        return nodes
        
        
    def remove_node (self, hostname=""):
        ## Remove node by hostname -- DOES NOST REMOVE THE MASTER
        nodes = []
        nodes.append(self.cluster[self.host_template+"master"])
        for i in range(1,len(self.cluster)):
            if not (self.host_template+str(i)).endswith(hostname):
                nodes.append(self.cluster[self.host_template+str(i)])
                
        self.my_logger.debug("New nodes:"+str( nodes))
        
        ## keep node
        node = self.cluster.pop(hostname)
        
        ## Kill all java processes on the removed node
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(node.public_dns_name, username='root', password='secretpw')
            stdin, stdout, stderr = ssh.exec_command('pkill java')
            ssh.close()
        except paramiko.SSHException:
            self.my_logger.debug("Failed to invoke shell!")
        
        ## Reconfigure cluster
        self.configure_cluster( nodes , self.host_template, True)
        self.my_logger.debug(str(self.cluster))
        sys.stdout.flush()
        
        ## Start the new configuration!
        self.start_cluster()
        
        ## Now you should be ok, so return the new node
        return node
    
    def rebalance_cluster (self, threshold = 0.1):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
##########
#        Different balancing options
#########

        ## Run compaction on all tables
        child = pexpect.spawn('ssh root@'+self.cluster[self.host_template+"master"].public_dns_name)
        child.expect ('password:')
        child.sendline ('secretpw')
        child.expect (':~#')
        child.sendline ('/opt/hbase-0.92.0/bin/hbase shell')
        child.expect ('0>')
        child.sendline ('list')
        got = child.readline()
        tables = []
        while got.find("row(s) in") == -1:
            if len(got) > 0:
                tables.append(got)
            got = child.readline()
        child.close()
        for table in tables:
            os.system("curl \"http://"+self.cluster[self.host_template+"master"].public_dns_name+":60010/table.jsp?action=compact&name="+table+"&key=\"")

       
        ## start HDFS balancer
#        ssh.connect(self.cluster[self.host_template+"master"].public_dns_name, username='root', password='secretpw')
#        stdin, stdout, stderr = ssh.exec_command('/opt/hadoop-0.20.2/bin/start-balancer.sh -threshold '+ str(threshold))
#        print stdout.readlines()
        
        ## set HBase balancing threshold in local file
#        shutil.copy("./templates/hbase/hbase-site.xml", "/tmp/hbase-site.xml")
#        hbase_site = "/tmp/hbase-site.xml"
#        
#        ## Remake the file with different balancing threshold
#        
#        ## make the quorum
#        for line in fileinput.FileInput(hbase_site,inplace=1):
#            line = line.replace("ZK_QUORUM_IPs", self.host_template+"master,"+self.host_template+str((len(self.cluster.keys())/2)+1)+","+self.host_template+str(len(self.cluster.keys()-1))).strip()
#            print line
#            
#        ## make the balancing
#        for line in fileinput.FileInput(hbase_site,inplace=1):
#            line = line.replace("0.1",).strip()
#            print line
#        
#        ## Upload new hbase file
#        for node in self.cluster.values():
#            transport = paramiko.Transport((node.public_dns_name, 22))
#            transport.connect(username = 'root', password = 'secretpw')
#            transport.open_channel("session", node.public_dns_name, "localhost")
#            sftp = paramiko.SFTPClient.from_transport(transport)
#            sftp.put( "/tmp/known_hosts", "/root/.ssh/known_hosts")
#            sftp.close()
#        
#        ## Restart hbase (stop-start)
#        ssh.connect(self.cluster[self.host_template+"master"].public_dns_name, username='root', password='secretpw')
#        stdin, stdout, stderr = ssh.exec_command('/opt/hbase-0.20.6/bin/stop-hbase.sh')
#        print stdout.readlines()
#        ssh.connect(self.cluster[self.host_template+"master"].public_dns_name, username='root', password='secretpw')
#        stdin, stdout, stderr = ssh.exec_command('/opt/hbase-0.20.6/bin/start-hbase.sh')
#        print stdout.readlines()
        

