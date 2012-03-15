'''
Created on Sep 30, 2010

@author: ikons
'''
#!/usr/bin/env python

import sys, os, time, atexit, logging.handlers
from signal import SIGTERM
import shutil, fileinput
import Utils

class Daemon:
        """
        A generic daemon class.
        Usage: subclass the Daemon class and override the run() method
        """

        def __init__(self, pidfile, stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
                self.stdin = stdin
                self.stdout = stdout
                self.stderr = stderr
                self.pidfile = pidfile
                
                ## Read properties from file
                self.utils = Utils.Utils()
                
                ## Make log directory
                os.system('mkdir -p '+self.utils.install_dir+"/logs")
                
                ## Install stdout
                self.stdout = self.utils.install_dir+"/logs/Coordinator.out"
                
                ## Install stderr
                self.stderr = self.utils.install_dir+"/logs/Coordinator.err"
                
                
                ## Install logger
                LOG_FILENAME = self.utils.install_dir+'/logs/Coordinator.log'
                self.my_logger = logging.getLogger('Deamon')
                self.my_logger.setLevel(logging.DEBUG)
                
                handler = logging.handlers.RotatingFileHandler(
                              LOG_FILENAME, maxBytes=2*1024*1024, backupCount=5)
                formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")
                handler.setFormatter(formatter)
                self.my_logger.addHandler(handler)
                
                
                ## log the properties loaded
                self.my_logger.debug(self.utils.euca_rc_dir)
                self.my_logger.debug(self.utils.initial_cluster_size)
                self.my_logger.debug(self.utils.bucket_name)
                self.my_logger.debug(self.utils.instance_type)
                self.my_logger.debug(self.utils.cluster_name)
                self.my_logger.debug(self.utils.hostname_template)
                self.my_logger.debug(self.utils.reconfigure)
                self.my_logger.debug(self.utils.cluster_type)
                self.my_logger.debug(self.utils.db_file)
                
                
                ## Set environment
                if self.utils.cloud_api_type == "EC2":
                    eucarc_file = open(self.utils.euca_rc_dir+"/eucarc", "r")
                    for line in eucarc_file.readlines():
                        if line.startswith("export"):
                            tokens = line.strip().split('=')
                            os.environ[tokens[0].replace("export ","")] = tokens[1].replace("${EUCA_KEY_DIR}", self.utils.euca_rc_dir).replace("'","")
                if self.utils.cloud_api_type == "EC2_OS":
                    eucarc_file = open(self.utils.euca_rc_dir+"/novarc", "r")
                    for line in eucarc_file.readlines():
                        if line.startswith("export"):
                            tokens = line.strip().split('=')
                            os.environ[tokens[0].replace("export ","")] = str(tokens[1].replace("${NOVA_KEY_DIR}", self.utils.euca_rc_dir).replace("'","").replace("\"",""))

                ## Return the environment with which the daemon is run
#                self.my_logger.debug(os.environ)
                
        
        def daemonize(self):
                
                """
                do the UNIX double-fork magic, see Stevens' "Advanced
                Programming in the UNIX Environment" for details (ISBN 0201563177)
                http://www.erlenstar.demon.co.uk/unix/faq_2.html#SEC16
                """
                
                try:
                        pid = os.fork()
                        if pid > 0:
                                # exit first parent
                                sys.exit(0)
                except OSError, e:
                        sys.stderr.write("fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))
                        sys.exit(1)
       
                # decouple from parent environment
#                os.chdir("/")
                os.chdir(self.utils.install_dir)
                os.setsid()
                os.umask(0)

                # do second fork
                try:
                        pid = os.fork()
                        if pid > 0:
                                # exit from second parent
                                sys.exit(0)
                except OSError, e:
                        sys.stderr.write("fork #2 failed: %d (%s)\n" % (e.errno, e.strerror))
                        sys.exit(1)
       
                # redirect standard file descriptors
                sys.stdout.flush()
                sys.stderr.flush()
                si = file(self.stdin, 'r')
                so = file(self.stdout, 'a+')
                #se = file(self.stderr, 'a+', 0)
                # uncomment the above line to redirect stderr
                
                os.dup2(si.fileno(), sys.stdin.fileno())
                os.dup2(so.fileno(), sys.stdout.fileno())
                #os.dup2(se.fileno(), sys.stderr.fileno())
                # uncomment the above line to redirect stderr
       
                # write pidfile
                atexit.register(self.delpid)
                pid = str(os.getpid())
                file(self.pidfile,'w+').write("%s\n" % pid)
       
        def delpid(self):
                os.remove(self.pidfile)
 
        def start(self):
                """
#
                Start the daemon
#
                """
                # Check for a pidfile to see if the daemon already runs
                try:
                        pf = file(self.pidfile,'r')
                        pid = int(pf.read().strip())
                        pf.close()
                except IOError:
                        pid = None
       
                if pid:
                        message = "pidfile %s already exist. Daemon already running?\n"
                        sys.stderr.write(message % self.pidfile)
                        sys.exit(1)
               
                # Start the daemon
                self.daemonize()
                self.run()
 
        def stop(self):
                """
#
                Stop the daemon
#
                """
                # Get the pid from the pidfile
                try:
                        pf = file(self.pidfile,'r')
                        pid = int(pf.read().strip())
                        pf.close()
                except IOError:
                        pid = None
       
                if not pid:
                        message = "pidfile %s does not exist. Daemon not running?\n"
                        sys.stderr.write(message % self.pidfile)
                        return # not an error in a restart
 
                # Try killing the daemon process       
                try:
                        while 1:
                                os.kill(pid, SIGTERM)
                                time.sleep(0.1)
                except OSError, err:
                        err = str(err)
                        if err.find("No such process") > 0:
                                if os.path.exists(self.pidfile):
                                        os.remove(self.pidfile)
                        else:
                                print str(err)
                                sys.exit(1)
 
        def restart(self):
                """
                Restart the daemon
                """
                self.stop()
                self.start()
 
        def run(self):
                """
                You should override this method when you subclass Daemon. It will be called after the process has been
                daemonized by start() or restart().
                """
                
       
            