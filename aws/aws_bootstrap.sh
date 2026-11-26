#!/bin/bash
# EC2 "User data" boot script for the LSST+Roman chunking benchmark (Ubuntu 24.04, x86_64).
# Runs once, as root, on first boot. Log: /var/log/cloud-init-output.log
set -euxo pipefail
export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y git curl unzip htop sysstat docker.io build-essential
usermod -aG docker ubuntu
systemctl enable --now docker
# AWS CLI v2 (uses the instance's IAM role automatically; no keys on disk)
curl -sSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o /tmp/awscliv2.zip
unzip -q /tmp/awscliv2.zip -d /tmp && /tmp/aws/install
# Miniforge (conda) for the ubuntu user
sudo -u ubuntu bash -c 'curl -sSL -o /tmp/mf.sh https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-x86_64.sh && bash /tmp/mf.sh -b -p $HOME/miniforge3 && $HOME/miniforge3/bin/conda init bash'
# Work dirs on the large root volume
sudo -u ubuntu mkdir -p /home/ubuntu/bench /home/ubuntu/lsst
echo "bootstrap done $(date -u)" > /home/ubuntu/BOOTSTRAP_DONE
