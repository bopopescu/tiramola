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

import sys
import shutil
import socket
import fileinput, paramiko, Utils
import xml.parsers.expat

class GParser:
    
    
    def __init__(self):
        self.inhost =0
        self.inmetric = 0
        self.allmetrics = {}
        self.currhostname=""
#        self.host = host
#        self.metric = metric

    def parse(self, file):
        p = xml.parsers.expat.ParserCreate()
#        p.StartElementHandler = parser.start_element
#        p.EndElementHandler = parser.end_element
        p.StartElementHandler = self.start_element
        p.EndElementHandler = self.end_element
#        print file.read()

        p.ParseFile(file)
        
#        print "inside function" , file
        if self.allmetrics == {}:
            raise Exception('Host/value not found')
        return self.allmetrics

    def start_element(self, name, attrs):
        # edo xtizo to diplo dictionary. vazo nodes kai gia kathe node vazo polla metrics.
        #print attrs
        if name == "HOST":
            #if attrs["NAME"]==self.host:
            self.allmetrics[attrs["NAME"]]={}
            # edo ftiaxno ena adeio tuple me key to onoma tou node kai value ena adeio dictionary object.
            self.inhost=1
            self.currhostname=attrs["NAME"]
#            print "molis mpika sto node me dns " , self.currhostname 
        
        elif self.inhost==1 and name == "METRIC": # and attrs["NAME"]==self.metric:
            #print "attrname: " , attrs["NAME"] , " attr value: " , attrs["VAL"]
            self.allmetrics[self.currhostname][attrs["NAME"]] = attrs["VAL"]

    def end_element(self, name):
            if name == "HOST" and self.inhost==1:
                self.inhost=0
#                print "molis vgika apo to node me dns " , self.currhostname
                self.currhostname=""
#                print self.allmetrics

class MonitorVms:
    def __init__(self, cluster):
        self.utils = Utils.Utils()
        self.cluster = cluster
        
        self.ganglia_host = ""
        self.ganglia_port = 8649
        
#        self.host = ""
        #metric = 'swap_free'
        self.configure_monitoring()
        self.allmetrics={};
        self.parser = GParser()
        # initialize parser object. in the refreshMetrics function call the .parse of the 
        # parser to update the dictionary object.
        
        


    def refreshMetrics(self):
        try:
            self.soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.soc.connect((self.ganglia_host,self.ganglia_port))
            self.allmetrics=self.parser.parse(self.soc.makefile("r"))
            self.soc.close()
        except socket.error:
            return self.allmetrics
        return self.allmetrics

    def configure_monitoring(self):
        ''' Gets a cluster objects and configures monitoring of the nodes using Ganglia. '''
        
        ## refresh cluster from db to make sure everything is correct in subsequent runs
        self.cluster = self.utils.get_cluster_from_db(self.utils.cluster_name)
        
        for clusterkey in self.cluster.keys():
            if clusterkey.endswith("master"):
                host_template = clusterkey.replace("master","")
                shutil.copy("./templates/ganglia/gmond.conf", "/tmp/gmond.conf")
                for line in fileinput.FileInput("/tmp/gmond.conf",inplace=1):
                    line = line.replace("CLUSTER_NAME",host_template+"cluster")
                    print line
                for line in fileinput.FileInput("/tmp/gmond.conf",inplace=1):
                    line = line.replace("GMETAD_IP", self.cluster[clusterkey].public_dns_name)
                    print line
                    
        for (clusterkey, node) in self.cluster.items():
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            print node.public_dns_name
            try:
                ssh.connect(node.public_dns_name, username='root', password='secretpw')
            except paramiko.SSHException:
                print "Failed to invoke shell!"
                continue
            transport = paramiko.Transport(node.public_dns_name)
            transport.connect(username = 'root', password = 'secretpw')
            transport.open_channel("session", node.public_dns_name, "localhost")
            sftp = paramiko.SFTPClient.from_transport(transport)
            sftp.put( "./templates/ganglia/gmetad.conf","/etc/gmetad.conf")
            sftp.put( "/tmp/gmond.conf","/etc/gmond.conf")
            sftp.put( "./templates/ganglia/gmetad.conf","/etc/ganglia/gmetad.conf")
            sftp.put( "/tmp/gmond.conf","/etc/ganglia/gmond.conf")
            sftp.close()
            stdin, stdout, stderr = ssh.exec_command('gmond')
            print stdout.readlines()
            stdin, stdout, stderr = ssh.exec_command('pkill gmond')
            print stdout.readlines()
            stdin, stdout, stderr = ssh.exec_command('gmond')
            
            ## Stop monitoring master infrastructure if running
            stdin, stdout, stderr = ssh.exec_command('pkill gmetad')
            print stdout.readlines()
            stdin, stdout, stderr = ssh.exec_command('/etc/init.d/apache2 stop')
            print stdout.readlines()
            
            print stdout.readlines()
            if clusterkey.endswith("master"):
                stdin, stdout, stderr = ssh.exec_command('gmetad')
                print stdout.readlines()
                stdin, stdout, stderr = ssh.exec_command('pkill gmetad')
                print stdout.readlines()
                stdin, stdout, stderr = ssh.exec_command('gmetad')
                print stdout.readlines()
                stdin, stdout, stderr = ssh.exec_command('/etc/init.d/apache2 restart')
                print stdout.readlines()
                stdin, stdout, stderr = ssh.exec_command('/etc/init.d/gmetad restart')
                print stdout.readlines()
                self.ganglia_host = node.public_dns_name
            ssh.close()
            
        


def usage():
    print """Usage: check_ganglia \
-h|--host= -m|--metric= -w|--warning= \
-c|--critical= [-s|--server=] [-p|--port=] """
    sys.exit(3)

if __name__ == "__main__":
##############################################################
#    ganglia_host = 'clusterhead'
#    ganglia_port = 8649
#    host = 'clusterhead'
#    metric = 'swap_free'
#    warning = None
#    critical = None


#    try:
#        options, args = getopt.getopt(sys.argv[1:],
#          "h:m:w:c:s:p:",
#          ["host=", "metric=", "warning=", "critical=", "server=", "port="],
#          )
#    except getopt.GetoptError, err:
#        print "check_gmond:", str(err)
#        usage()
#        sys.exit(3)
#
#    for o, a in options:
#        if o in ("-h", "--host"):
#            host = a
#        elif o in ("-m", "--metric"):
#            metric = a
#        elif o in ("-w", "--warning"):
#            warning = float(a)
#        elif o in ("-c", "--critical"):
#            critical = float(a)
#        elif o in ("-p", "--port"):
#            ganglia_port = int(a)
#        elif o in ("-s", "--server"):
#            ganglia_host = a
#
#    if critical == None or warning == None or metric == None or host == None:
#        usage()
#        sys.exit(3)
    myUtil = Utils.Utils()
    mycluster = {}
    mycluster["db-image-master"] = myUtil.return_instance_from_tuple(("i-1","emi-1","62.217.120.125","62.217.120.125","running","eangelou","0","fdsf","c1.xlarge","sometime","Centos","eki-1","eri-1"))
    
    monVms = MonitorVms(mycluster)
    allmetrics=monVms.refreshMetrics()
    print "allmetrics: ", allmetrics
    allmetrics=monVms.refreshMetrics()
    print "allmetrics2: ", allmetrics

#    allmetrics=monVms.refreshMetrics()
#    print "allmetrics length ", len(allmetrics)


#    try:
#        print "ganglia host " + ganglia_host
#        print 'host ' + host
#        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#        s.connect((ganglia_host,ganglia_port))
##        file = s.makefile("r")
##        print file.read()
#        parser = GParser(host, metric)
##        print "outside function" , s.makefile("r")
#        value = parser.parse(s.makefile("r"))
#        
#        s.close()
#    except Exception, err:
#        print "CHECKGANGLIA UNKNOWN: Error while getting value \"%s\"" % (err)
#        sys.exit(3)
#
#    if value >= critical:
#        print "CHECKGANGLIA CRITICAL: %s is %.2f" % (metric, value)
#        sys.exit(2)
#    elif value >= warning:
#        print "CHECKGANGLIA WARNING: %s is %.2f" % (metric, value)
#        sys.exit(1)
#    else:
#        print "CHECKGANGLIA OK: %s is %.2f" % (metric, value)
#        sys.exit(0)