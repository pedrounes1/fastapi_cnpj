#! /usr/bin/env bash

apt update -y
apt upgrade -y
apt install -y python-pip
pip install -r ./requirements.txt