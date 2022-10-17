# Apache Spark Installation steps    
The following are the steps to install **Apache Spark** into a Linux VM in Azure. This also provides steps on how we can access **Jupyter Notebook** in the remote machine from our local desktop         


## Create the Resource Group          
az group create --name agsparkrg --location eastus

## Create the VM        
az vm create \
--resource-group agsparkrg \
--name agsparkVM \
--image UbuntuLTS \
--admin-username <USERNAME> \
--admin-password <PASSWORD>

## Open the PORT             
az vm open-port --port 8888 --resource-group agsparkrg --name agsparkVM

## ssh into the vm         
ssh ambarishspark@<MACHINE PUBLIC IP>

## Install and Configure Jupyter Notebook            
sudo apt update
sudo apt-get install software-properties-common
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt-get update
sudo apt-get install python3.8
sudo apt install python3-pip
pip3 install --upgrade setuptools
pip3 install jupyter
export PATH=$PATH:~/.local/bin

## Install Apache Spark                  
wget https://dlcdn.apache.org/spark/spark-3.3.0/spark-3.3.0-bin-hadoop3.tgz

tar -xf spark-3.3.0-bin-hadoop3.tgz
pip3 install pyspark

## Install JDK     
sudo apt install default-jdk

export JAVA_HOME="/usr/lib/jvm/default-java"

## Configure to access Jupyter Notebook from remote machine          
ssh -L 8888:localhost:8888 your_server_username@your_server_ip
ssh -L 8888:localhost:8888 ambarishspark@4.246.194.76

## Invoke Jupyter Notebook               
jupyter notebook --no-browser 

## Delete the Resource Group               
az group delete --name agsparkrg 