# Prerequisites #

TIRAMOLA is written in Python 2.6 and uses the boto library. Therefore, it should be used in a machine with the following prerequisites:
  * python >= 2.6 (has NOT been tested with Python 3.0)
  * python-boto >= 1.9  (if you have euca2ools >= 1.1 installed, you are probably ok)
  * python-paramiko >= 1.7

# Using a suitable image #

Strictly speaking, any usable image will work, however TIRAMOLA expects the following structure for the image contents:
  * All NoSQL DBs should be put under the /opt directory. Supported are hbase-0.20.6, apache-cassandra-0.7.0-beta1 and voldemort-0.81. Therefore the expected structure is:
    * /opt/apache-cassandra-0.7.0-beta1
    * /opt/hadoop-0.20.2
    * /opt/hbase-0.20.6
    * /opt/voldemort-0.81
  * To use the monitoring infrastructure, ganglia has to be installed on the machine and accessible from init.d scripts. You can use any version available to you, though we have tested with 3.0.7.

To give you an idea, and to kickstart your experience, use the precooked image in the 'resources' directory (you can get it by downloading [this torrent](http://tiramola.googlecode.com/files/tiramola-resources.tar.gz.6313292.TPB.torrent)). You have to upload it to the Cloud yourself. E.g., using euca2ools:
```
## bundle kernel
$ euca-bundle-image -i vmlinuz-2.6.32-5-amd64 --kernel true
$ euca-upload-bundle -b mybucket -m /tmp/vmlinuz-2.6.32-5-amd64.manifest.xml
$ euca-register mybucket/vmlinuz-2.6.32-5-amd64.manifest.xml
## KEEP THE OUTPUT OF THE LAST COMMAND AS EKI
## bundle ramdisk
$ euca-bundle-image -i initrd.img-2.6.32-5-amd64 --ramdisk true
$ euca-upload-bundle -b mybucket -m /tmp/initrd.img-2.6.32-5-amd64.manifest.xml
$ euca-register mybucket/initrd.img-2.6.32-5-amd64.manifest.xml
## KEEP THE OUTPUT OF THE LAST COMMAND AS ERI
## bindle image
$ euca-bundle-image -i db-image-all-squeeze-comp.img --kernel $EKI --ramdisk $ERI
$ euca-upload-bundle -b mybucket -m /tmp/db-image-all-squeeze-comp.img.manifest.xml
$ euca-register mybucket/db-image-all-squeeze-comp.img.manifest.xml
```

You should be ok if you can launch instances of this instance.


# Running TIRAMOLA for the first time #

TIRAMOLA should be configured for the first time. Once you have checked-out the latest code, copy the file Coordinator.properties to your home directory or /etc/. Parameterize it as needed, most options are self-explanatory and you can mess around. You can choose which decision making module to use by setting the self.decisionMaker member variable of the Coordinator class. Have a look at DecisionMaking for the available options. Once you have configured TIRAMOLA, run the service from the src/ directory using "python Coordinator.py start". You can see the logs in logs/Coordinator{.log|.out}.