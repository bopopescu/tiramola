''' Basic imports to start the procedure '''

import boto
import boto.ec2
import Utils
from euca2ools import Euca2ool, InstanceValidationError, ConnectionFailed, FileValidationError
import getopt
from pysqlite2 import dbapi2 as sqlite
import sys, os, time
import EucaCluster, HBaseCluster, MonitorVms, RiakCluster, CassandraCluster

def main():
	
	## Manually truncate known instances
#	con = sqlite.connect("/tmp/eMRE.db")
#	cur = con.cursor()
#	try:
#		cur.execute('delete from instances'
#	            )
#		con.commit()
#	except sqlite.DatabaseError:
#		print "ERROR in truncate"
#	
#	# Assume running when eucarc sourced 
#	mycluster = EucaCluster.EucaCluster()
#	instances = mycluster.describe_instances()
##	print instances
##	print mycluster.describe_instances("pollDB", "i-32F306F5")[0].public_dns_name
#	myUtil = Utils.Utils()
##	print myUtil.query_instance_db("i-525F09E8")
#	
#	
##	print mycluster.describe_instances("running","i-385")
#	
#	images = mycluster.describe_images("db-full-comp")
#	print images
#	instances = mycluster.run_instances(images[0].id, None, None, None, 2, 2, "c1.xlarge")
#	print instances
#	## Run describe instances until everyone is running
##	all_instances = mycluster.describe_instances()
##	running_instances = mycluster.describe_instances("running")
##	i=0
##	while not (len(all_instances) == len(running_instances)):
##		print "Run:", i
##		time.sleep(120)
##		all_instances = mycluster.describe_instances()
##		running_instances = mycluster.describe_instances("running")
##		i += 1
##	instances = running_instances
#	instances = mycluster.block_until_running(instances)
#	print instances
#	# Terminate all running instances
##	instances = mycluster.describe_instances("cluster")
##
##	for instance in instances:
##		mycluster.terminate_instances([instance.id])
#	
#
#
#
#
#		
#	## Add instances manually
#	#instances = []
#	#instances.append(myUtil.return_instance_from_tuple(("i-1","emi-1","147.102.3.218","147.102.3.218","running","eangelou","0","fdsf","c1.xlarge","sometime","Centos","eki-1","eri-1")))
#	#instances.append(myUtil.return_instance_from_tuple(("i-2","emi-1","147.102.3.204","147.102.3.204","running","eangelou","1","fdsf","c1.xlarge","sometime","Centos","eki-1","eri-1")))
#	#myUtil.add_to_instance_db(instances)
#	
#	## Check config cluster
#	#myCluster = HBaseCluster.HBaseCluster("eangelou-hbase")
#	myCluster = CassandraCluster.CassandraCluster("eangelou-cassandra")
#	
#	print myCluster.configure_cluster(instances, "cassandra-nodes-", False)
#	time.sleep(60)
#	myCluster.start_cluster()
#	myCluster.start_cluster()
##	instance = mycluster.run_instances(images[0].id, None, None, None, 1, 1, "c1.xlarge")
#	
#	time.sleep(240)
#	
#	new_node = mycluster.run_instances(images[0].id, None, None, None, 1, 1, "c1.xlarge")
#	print new_node
#	
#	new_node = mycluster.block_until_running(new_node)
#	print new_node
#	
#	myCluster.add_node(new_node[0])
#	
#	vmMonitor = MonitorVms.MonitorVms(myCluster.cluster)
#	allmetrics=vmMonitor.refreshMetrics()
#	print "allmetrics: ", allmetrics
#	allmetrics=vmMonitor.refreshMetrics()
#	print "allmetrics2: ", allmetrics
	
#	myCluster.add_node(instance[0])
#	myCluster.remove_node("manual-nodes-2")

#	ssh = paramiko.SSHClient()
#	ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#	ssh.connect("62.217.120.115", username='root', password='secretpw')
#	stdin, stdout, stderr = ssh.exec_command('/opt/apache-cassandra-0.7.0-beta1/bin/nodetool -host ' +
#	                                          "62.217.120.115" +' ring')
#	rebalance_ip = ""
#	var_max = 0
#	for line in stdout.readlines():
#		if not line.startswith("Address") and not line.startswith(" "):
#			splits = line.split()
#			var = float(splits[3].replace("MB",""))
#			if var_max < var:
#				var_max = var
#				rebalance_ip = splits[0]
#	print "Rebalancing node with ip: ", rebalance_ip
#
#	stdin, stdout, stderr = ssh.exec_command('/opt/apache-cassandra-0.7.0-beta1/bin/nodetool -host ' +
#	                                          rebalance_ip +' loadbalance')
#	print stdout.readlines()
#	ssh.close()
#	
	## Delete host from cluster db
	con = sqlite.connect("/home/vagos/eMRE.hbase.db")
	cur = con.cursor()
	hostname = "hbase-nodes-8"
	
	cur.execute('delete from clusters where hostname = \"'+hostname+"\""
                    )
	
	con.commit()
#	cur.execute('create table clusters(cluster_id text, hostname text, euca_id text)')
#	con.commit()
	
	cur.execute(""" insert into clusters(cluster_id, hostname, euca_id ) 
                                                    values  (?,?,?)""",
                            ("vagos-hbase", hostname, "i-49BB095E")
                            )
	con.commit()
	
	cur.close()
	con.close()
	
#		## Delete host from cluster db
#	con = sqlite.connect("/tmp/eMRE.db")
#	cur = con.cursor()
#	hostname = "hbase-nodes-3"
#	
#	
#	cur.execute('delete from clusters where cluster_id = \"vagos-cassandra\"'
#                    )
#	
#	cur.execute(""" insert into clusters(cluster_id, hostname, euca_id ) 
#                                                    values  (?,?,?)""",
#                            ("vagos-cassandra", "cassandra-nodes-master", "i-3E260748")
#                            )
#	con.commit()
#		
#	cur.execute(""" insert into clusters(cluster_id, hostname, euca_id ) 
#                                                    values  (?,?,?)""",
#                            ("vagos-cassandra", "cassandra-nodes-1", "i-4FCD098A")
#                            )
#	con.commit()
#		
#	cur.execute(""" insert into clusters(cluster_id, hostname, euca_id ) 
#                                                    values  (?,?,?)""",
#                            ("vagos-cassandra", "cassandra-nodes-2", "i-349C0712")
#                            )
#	con.commit()
#	cur.execute(""" insert into clusters(cluster_id, hostname, euca_id ) 
#                                                    values  (?,?,?)""",
#                            ("vagos-cassandra", "cassandra-nodes-3", "i-45BB07BE")
#                            )
#	con.commit()
#	
#	
#	cur.close()
#	con.close()
	
	
#	con = sqlite.connect("/tmp/eMRE.db")
#	cur = con.cursor()
#	
#	
#	cur.execute('delete from clusters where cluster_id = \"vagos-hbase\"'
#                    )
#	
#	cur.execute(""" insert into clusters(cluster_id, hostname, euca_id ) 
#                                                    values  (?,?,?)""",
#                            ("vagos-hbase", "hbase-nodes-master", "i-319C05AE")
#                            )
#	con.commit()
#		
#	cur.execute(""" insert into clusters(cluster_id, hostname, euca_id ) 
#                                                    values  (?,?,?)""",
#                            ("vagos-hbase", "hbase-nodes-1", "i-31D60651")
#                            )
#	con.commit()
#		
#	cur.execute(""" insert into clusters(cluster_id, hostname, euca_id ) 
#                                                    values  (?,?,?)""",
#                            ("vagos-hbase", "hbase-nodes-2", "i-4C4D0941")
#                            )
#	con.commit()
#	cur.execute(""" insert into clusters(cluster_id, hostname, euca_id ) 
#                                                    values  (?,?,?)""",
#                            ("vagos-hbase", "hbase-nodes-3", "i-4B850863")
#                            )
#	con.commit()
#	cur.execute(""" insert into clusters(cluster_id, hostname, euca_id ) 
#                                                    values  (?,?,?)""",
#                            ("vagos-hbase", "hbase-nodes-4", "i-3FED087F")
#                            )
#	con.commit()
#	
#	
#	cur.close()
#	con.close()
	
if __name__ == "__main__":
	main()

