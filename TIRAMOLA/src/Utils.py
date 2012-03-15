'''
Created on Jun 8, 2010

@author: vagos
'''

#import paramiko
import re
#import boto
import boto.ec2
#from euca2ools import Euca2ool, InstanceValidationError, ConnectionFailed, FileValidationError
from pysqlite2 import dbapi2 as sqlite
#import sys, os
import os
from ConfigParser import ConfigParser

class Utils(object):
    '''
    This class holds utility functions. 
    '''
    
    def __init__(self):
        self.read_properties(os.getenv("HOME", "/etc") + "/Coordinator.properties")


    def return_instance_from_tuple(self, tuple):
        instance = boto.ec2.instance.Instance(tuple[0])
        instance.id = tuple[0]
        instance.image_id = tuple[1]
        instance.public_dns_name = tuple[2]
        instance.private_dns_name = tuple[3]
        instance.state = tuple[4]
        instance.key_name = tuple[5]
        instance.ami_launch_index = tuple[6]
        instance.product_codes = tuple[7]
        instance.instance_type = tuple[8]
        instance.launch_time = tuple[9]
        instance.placement = tuple[10]
        instance.kernel = tuple[11]
        instance.ramdisk = tuple[12]
        return instance
        
        
    def query_instance_db(self, pattern):
        """ A helpful search for the sqlite db - returns instances"""
        search_field = None
        search_field1 = None
        
        
        if re.match('i-', pattern):
        ## looking by instance id
            search_field = "id"
        else:
            if re.match('emi-', pattern):
            ## looking by image id
                search_field = "image_id"
            else:
                if pattern.find(".") != -1:
                    ## looking by ip
                    search_field = "public_dns_name"
                    search_field1 = "private_dns_name"
                else:
                    ## looking by state
                    search_field = "state"

        instances = []
        ## read from the local database
        con = sqlite.connect(self.db_file)
        cur = con.cursor()
        instancesfromDB = []
        if search_field1 :
            try:
                instancesfromDB = cur.execute('select * from instances where ' + search_field +"=\"" +
                                              pattern + "\" OR " + search_field1 + "=\"" + pattern + "\""
                            ).fetchall()
            except sqlite.DatabaseError:
                con.rollback()
        else:
            try:
                instancesfromDB = cur.execute('select * from instances where ' + search_field +"=\"" +
                                              pattern + "\"").fetchall()
            except sqlite.DatabaseError:
                con.rollback()              
            
        cur.close()
        con.close()
        
        for instancefromDB in instancesfromDB:
            instances.append(self.return_instance_from_tuple(instancefromDB))
                
        return instances
    
    def refresh_instance_db(self, instances):
        ## Update instance DB with provided instances (removes all previous entries!)
        con = sqlite.connect(self.db_file)
        cur = con.cursor()
        try:
            cur.execute('delete from instances'
                    )
            con.commit()
        except sqlite.DatabaseError:
            print "ERROR in truncate"
            
        for instance in instances:
            try:
                cur.execute(""" insert into instances(id, image_id, public_dns_name, private_dns_name,state,
                                                   key_name, ami_launch_index, product_codes,instance_type,
                                                   launch_time, placement, kernel, ramdisk ) 
                                                    values  (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                            (instance.id, instance.image_id,instance.public_dns_name,instance.private_dns_name, instance.state,
                                                            instance.key_name,instance.ami_launch_index,instance.product_codes,instance.instance_type,
                                                            instance.launch_time,instance.placement,instance.kernel,instance.ramdisk)
                            )
                con.commit()
            except sqlite.DatabaseError, e:
                print e.message
                print "ERROR in insert"

        cur.close()
        con.close()
        
    def add_to_instance_db(self, instances):
        ## Update instance DB with provided instances (keeps previous entries!)
        con = sqlite.connect(self.db_file)
        cur = con.cursor()
            
        for instance in instances:
            try:
                cur.execute(""" insert into instances(id, image_id, public_dns_name, private_dns_name,state,
                                                   key_name, ami_launch_index, product_codes,instance_type,
                                                   launch_time, placement, kernel, ramdisk ) 
                                                    values  (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                            (instance.id, instance.image_id,instance.public_dns_name,instance.private_dns_name, instance.state,
                                                            instance.key_name,instance.ami_launch_index,instance.product_codes,instance.instance_type,
                                                            instance.launch_time,instance.placement,instance.kernel,instance.ramdisk)
                            )
                con.commit()
            except sqlite.DatabaseError, e:
                print e.message
                print "ERROR in insert"

        cur.close()
        con.close()
        
    ########################################################
    ##     Cluster DB functions
    ########################################################
    
    def delete_cluster_from_db(self, clusterid = "default"):
        con = sqlite.connect(self.db_file)
        cur = con.cursor()
        try:
            cur.execute('delete from clusters where cluster_id=\"'+clusterid+"\"")
            con.commit()
        except sqlite.DatabaseError:
            print "ERROR in truncate"
        cur.close()
        con.close()
        
    def refresh_cluster_db(self, cluster=None):
        ## Update cluster DB with provided cluster (removes all previous entries!)
        con = sqlite.connect(self.db_file)
        cur = con.cursor()
        try:
            cur.execute('delete from clusters'
                    )
            con.commit()
        except sqlite.DatabaseError:
            print "ERROR in truncate"
            
        for (clusterkey,clustervalue) in cluster.items():
            try:
                cur.execute(""" insert into clusters(cluster_id, hostname, euca_id ) 
                                                    values  (?,?,?)""",
                            ("default", clusterkey, clustervalue.id)
                            )
                con.commit()
            except sqlite.DatabaseError, e:
                print e.message
                print "ERROR in insert"

        cur.close()
        con.close()
    
    def get_cluster_from_db(self, cluster_id=None):
        if not cluster_id:
            print "Got to provide cluster id!!!"
        else:
            con = sqlite.connect(self.db_file)
            cur = con.cursor()
            try:
                clusterfromDB = cur.execute('select * from clusters where cluster_id = \"'+cluster_id+"\""
                        ).fetchall()
                con.commit()
            except sqlite.DatabaseError:
                print "ERROR in select"
                return None
            
            if len(clusterfromDB) < 1:
                print "Have not found the requested cluster - exiting."
            else:
                ## build a cluster object
                cluster = {}
                for clusternode in clusterfromDB:
                    print (clusternode[2])
                    ## query db to get the corresponding instance
                    instance = self.query_instance_db(clusternode[2])
                    ## Populate cluster if instance in db
                    if instance:
                        cluster[clusternode[1]] = instance[0]
#                        print cluster
#                        print "Instance:", instance
#                        sys.stdout.flush()
                return cluster
        return None
    
    def add_to_cluster_db(self, cluster=None, cluster_id=None):
        ## Add cluster to DB (check for existing records with the same id and remove)
        con = sqlite.connect(self.db_file)
        cur = con.cursor()
            
        try:
            cur.execute('delete from clusters where cluster_id = \"'+cluster_id+"\""
                    )
            con.commit()
        except sqlite.DatabaseError:
            print "No previous entries"
            
        for (clusterkey,clustervalue) in cluster.items():
            try:
                cur.execute(""" insert into clusters(cluster_id, hostname, euca_id ) 
                                                    values  (?,?,?)""",
                            (cluster_id, clusterkey, clustervalue.id)
                            )
                con.commit()
            except sqlite.DatabaseError, e:
                print e.message
                print "ERROR in insert"

        cur.close()
        con.close()
    
    def rem_from_cluster_db(self, cluster_id=None, hostname=None):
        ## Add cluster to DB (check for existing records with the same id and remove)
        con = sqlite.connect(self.db_file)
        cur = con.cursor()
            
        try:
            cur.execute('delete from clusters where cluster_id = \"'+cluster_id+"\" and hostname = \""+hostname+"\"" 
                    )
            con.commit()
        except sqlite.DatabaseError:
            print "Error in delete"
            
        cur.close()
        con.close()
    
    def read_properties(self, property_file="Coordinator.properties"):
            """ process properties file """
            ## Reads the configuration properties
            cfg = ConfigParser()
            cfg.read(property_file)
            self.install_dir = cfg.get("config", "install_dir")
            self.euca_rc_dir = cfg.get("config", "euca_rc_dir")
            self.initial_cluster_size = cfg.get("config", "initial_cluster_size")
            self.max_cluster_size = cfg.get("config", "max_cluster_size")
            self.bucket_name = cfg.get("config", "bucket_name")
            self.instance_type = cfg.get("config", "instance_type")
            self.cluster_name = cfg.get("config", "cluster_name")
            self.hostname_template = cfg.get("config", "hostname_template")
            self.reconfigure = cfg.get("config", "reconfigure")
            self.cluster_type = cfg.get("config", "cluster_type")
            self.db_file = cfg.get("config", "db_file")
            self.add_nodes = cfg.get("config", "add_nodes")
            self.rem_nodes = cfg.get("config", "rem_nodes")
            self.cloud_api_type = cfg.get("config", "cloud_api_type")
            self.trans_cost = cfg.get("config", "trans_cost")
            self.gain = cfg.get("config", "gain")
            self.serv_throughput = cfg.get("config", "serv_throughput")
            try:
                self.gamma = cfg.get("config", "gamma")
            except:
                self.gamma = 0
            
            ## Reads the monitoring thresholds
            self.thresholds_add = {}
            self.thresholds_remove = {}
            for option in cfg.options("thresholds_add"):
                self.thresholds_add[option] = cfg.get("thresholds_add", option)
            for option in cfg.options("thresholds_remove"):
                self.thresholds_remove[option] = cfg.get("thresholds_remove", option)
            
            