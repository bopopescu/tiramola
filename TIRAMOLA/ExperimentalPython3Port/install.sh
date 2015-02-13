#!/bin/bash
#
# Helpful script to easily install package dependencies in ubuntu
#

# Ubuntu saucy
sudo aptitude install python3-pip python3-matplotlib python3-zmq python3-openssl python3-mpi4py  python3-sqlalchemy python3-crypto python3-setuptools python3-dev

sudo pip3 install -r requirements-python3.txt
# IPv6 mirror: sudo pip3 install -i http://pypi.gocept.com/simple -r requirements-python3.txt
# Also see http://www.pypi-mirrors.org/ as they might change...


# Optional utils (recommended)
sudo aptitude install sqliteman

