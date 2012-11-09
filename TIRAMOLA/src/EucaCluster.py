'''
Created on Jun 8, 2010

@author: vagos
'''
import paramiko
import boto.ec2
from euca2ools.commands.euca import describeimages, describeinstances, runinstances, terminateinstances
from pysqlite2 import dbapi2 as sqlite
import sys, os, time
import Utils
import commands
from boto.ec2.connection import EC2Connection

class EucaCluster(object):
    '''
    This class holds all instances that take part in the virtual cluster.
    It can create and stop new instances - and working in conjuction with the
    db specific classes set up various environments. 
    '''
    def __init__(self):
        '''
        Constructor
        ''' 
        self.utils = Utils.Utils()
        
#        Make sure the sqlite file exists. if not, create it and the table we need
        con = sqlite.connect(self.utils.db_file)
        cur = con.cursor()
        try:
            instances = cur.execute('select * from instances'
                            ).fetchall()
            print """Already discovered instances from previous database file. Use describe_instances without arguments to update.
            """
            print "Found records:\n", instances
        except sqlite.DatabaseError:
            cur.execute('create table instances(id text, image_id text, public_dns_name text, private_dns_name text,state text, key_name text, ami_launch_index text, product_codes text,instance_type text, launch_time text, placement text, kernel text, ramdisk text)')
            con.commit()
            
        cur.close()
        con.close()
        
        
        
    def describe_instances(self, state=None, pattern=None):
        instances = []
        
        if state != "pollDB":
            # Euca-describe-instances
            describeCmd = describeinstances.DescribeInstances()
            # get the boto connection
            myconn =  describeCmd.make_connection()
            reservations =  myconn.get_all_instances();
            
            members = ("id", "image_id", "public_dns_name", "private_dns_name",
        "state", "key_name", "ami_launch_index", "product_codes",
        "instance_type", "launch_time", "placement", "kernel",
        "ramdisk")
            
            for reservation in reservations:
                for instance in reservation.instances:
                    details = {}
                    for member in members:
                        val = getattr(instance, member, "")
                        # product_codes is a list
                        if val is None: val = ""
                        if hasattr(val, '__iter__'):
                            val = ','.join(val)
                        details[member] = val.partition('\n')[0].strip()
                    for var in details.keys():
                        exec "instance.%s=\"%s\"" % (var, details[var])
                    if state:
                        if state == instance.state:
                            instances.append(instance)
                    else:
                        instances.append(instance)
                        
            ## if simple call
            if not state:
                self.utils.refresh_instance_db(instances)
                        
        else :
            ## read from the local database
            con = sqlite.connect(self.utils.db_file)
            cur = con.cursor()
            instancesfromDB = []
            try:
                instancesfromDB = cur.execute('select * from instances'
                            ).fetchall()
            except sqlite.DatabaseError:
                con.rollback()
                
            cur.close()
            con.close()
            
            
            for instancefromDB in instancesfromDB:
                instances.append(self.utils.return_instance_from_tuple(instancefromDB))
        
        ## if you are using patterns and state, show only matching state and id's
        matched_instances = []
        if pattern:
            for instance in instances:
                if instance.id.find(pattern) != -1:
                    matched_instances.append(instance)
                    
            if len(matched_instances) > 0:
                return matched_instances
            else:
                return None
        else:
            return instances

        
    def describe_images(self, pattern=None):
        # Euca-describe-images
        describeCmd =  describeimages.DescribeImages()
        # get the boto connection
        myconn =  describeCmd.make_connection()
        images = myconn.get_all_images()
        
        print images
        
        ## if you are using patterns, show only matching names and emi's
        matched_images = []
        if pattern:
            for image in images:
                if image.name.find(pattern) != -1 and image.id.find("ami") != -1:
                    matched_images.append(image)
#        else:
#            print images[1].location
            if len(matched_images) > 0:
                return matched_images
            else:
                return None
        else:
            return images
        
    def read_user_data(self, user_data_filename):
        USER_DATA_CHUNK_SIZE = 512
        user_data = "";
        user_data_file = open(user_data_filename, "r")
        while 1:
            data = user_data_file.read(USER_DATA_CHUNK_SIZE)
            if not data:
                break
            user_data += data
        user_data_file.close()
        return user_data
    
    def run_instances(self, image_id=None,
        keyname=None,
        kernel_id=None,
        ramdisk_id=None,
        min_count=1,
        max_count=1,
        instance_type='m1.small',
        group_names=[],
        user_data=None,
        user_data_file=None,
        addressing_type="public",
        zone=None): 
        # euca-run-instances
        runCmd = runinstances.RunInstances()
        myconn =  runCmd.make_connection()
        reservation = myconn.run_instances(image_id=image_id,  
                                           min_count=min_count,  
                                           max_count=max_count,
                                           key_name=keyname,
                                           security_groups=group_names,
                                           instance_type=instance_type,
                                           kernel_id=kernel_id,
                                           ramdisk_id=ramdisk_id)
        
        print reservation
            
#        print reservation.id
        instances = []
        
        ## add the newly run instances to the database
        members = ("id", "image_id", "public_dns_name", "private_dns_name",
        "state", "key_name", "ami_launch_index", "product_codes",
        "instance_type", "launch_time", "placement", "kernel",
        "ramdisk")
        
        for instance in reservation.instances:
            ## get instance details
            details = {}
            for member in members:
                val = getattr(instance, member, "")
                # product_codes is a list
                if val is None: val = ""
                if hasattr(val, '__iter__'):
                    val = ','.join(val)
                details[member] = val
            for var in details.keys():
                exec "instance.%s=\"%s\"" % (var, details[var])
                
            instances.append(instance)
                    
        self.utils.add_to_instance_db(instances)
        
        return instances
        


        
    def terminate_instances(self, instance_ids):
        terminateCmd = terminateinstances.TerminateInstances()
        myconn =  terminateCmd.make_connection()
        instances = myconn.terminate_instances(instance_ids)
        return instances
        

    
#         
## Utilities
#   
    def block_until_running (self, instances):
        ''' Blocks until all defined instances have reached running state and an ip has been assigned'''
        ## Run describe instances until everyone is running
        tmpinstances = instances
        instances = []
        while len(tmpinstances) > 0 :
            time.sleep(30)
            print "Waiting for", len(tmpinstances), "instances."
            sys.stdout.flush()
            all_running_instances = self.describe_instances("running")
#            print all_running_instances
#            print tmpinstances
            for i in range(0,len(all_running_instances)):
                for j in range(0,len(tmpinstances)):
                    if (all_running_instances[i].id == tmpinstances[j].id) and (not (all_running_instances[i].public_dns_name == "0.0.0.0")):
                        tmpinstances.pop(j)
                        instances.append(all_running_instances[i])
                        break
        self.describe_instances()
        return instances
        
            
if __name__ == "__main__":
    euca = EucaCluster()
    euca.describe_instances("cluster")
