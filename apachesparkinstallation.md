az group create --name agsparkrg --location eastus

az vm create \
--resource-group agsparkrg \
--name agsparkVM \
--image UbuntuLTS \
--admin-username ambarishspark \
--admin-password Ambarishspark@1234 

az vm open-port --port 8888 --resource-group agsparkrg --name agsparkVM

ssh ambarishspark@4.246.194.76

sudo apt update
sudo apt-get install software-properties-common
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt-get update
sudo apt-get install python3.8
sudo apt install python3-pip
pip3 install --upgrade setuptools
pip3 install jupyter
export PATH=$PATH:~/.local/bin

wget https://dlcdn.apache.org/spark/spark-3.3.0/spark-3.3.0-bin-hadoop3.tgz

tar -xf spark-3.3.0-bin-hadoop3.tgz
pip3 install pyspark


sudo apt install default-jdk

export JAVA_HOME="/usr/lib/jvm/default-java"

ssh -L 8888:localhost:8888 your_server_username@your_server_ip
ssh -L 8888:localhost:8888 ambarishspark@4.246.194.76

jupyter notebook --no-browser 

az group delete --name agsparkrg 