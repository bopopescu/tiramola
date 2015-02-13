#!/bin/bash
#
# Helpful script to easily install package dependencies in fedora
#

# Ubuntu saucy
sudo yum install python3-pip python3-matplotlib python3-zmq python3-pyOpenSSL python3-mpi4py-openmpi  python3-sqlalchemy python3-crypto python3-setuptools python3-devel python3-matplotlib-tk

sudo pip-python3 install -r requirements-python3.txt
# IPv6 mirror: sudo pip3 install -i http://e.pypi.python.org/simple -r requirements-python3.txt

# Optional utils (recommended)
sudo yum install sqliteman

