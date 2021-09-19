#! /usr/bin/env bash

apt update -y
apt upgrade -y
apt install -y python3-pip
pip install -r ./requirements.txt