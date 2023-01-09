#!/usr/bin/env bash

getNow()
{
        echo "`date '+%Y-%m-%d %H:%M:%S'`"
}

getNowTimestamp()
{
		echo "`date '+%s'`"
}



# SET VARIABLE
EXEC_DT="`date '+%Y%m%d_%H%M%S'`"
RUN_FILE=$0
RUN_FILE_REALPATH=`realpath ${RUN_FILE}`
RUN_FILE_NAME=`basename ${RUN_FILE_REALPATH}`
RUN_FILE_DIR=`dirname ${RUN_FILE_REALPATH}`
RUNNER=`whoami`

ZK_LOG_FILE="/home/ec2-user/log/zookeeper/zookeeper.log"
PN1_LOG_FILE="/home/ec2-user/log/pinot/pinot-controller.log"
PN2_LOG_FILE="/home/ec2-user/log/pinot/pinot-broker.log"
PN3_LOG_FILE="/home/ec2-user/log/pinot/pinot-server.log"
SP_LOG_FILE="/home/ec2-user/log/superset/superset.log"
KFK_LOG_FILE="/home/ec2-user/log/kafka/kafka.log"
DRD_LOG_FILE="/home/ec2-user/log/druid/druid.log"


# env on program start 
echo -e "[$(getNow)] ${RUN_FILE_NAME} Start. \n"

# docker start
sudo systemctl start docker 
echo -e "[$(getNow)] docker Start. \n"

# zookeeper start
docker start zookeeper 
sudo docker logs zookeeper > ${ZK_LOG_FILE} 2>&1
echo -e "[$(getNow)] zookeeper Start. log diretory is ${ZK_LOG_FILE} \n"


# pinot-controller start
docker start pinot-controller
sudo docker logs pinot-controller > ${PN1_LOG_FILE} 2>&1
echo -e "[$(getNow)] pinot-controller Start. log diretory is ${PN1_LOG_FILE} \n"


# pinot-broker start
docker start pinot-broker
sudo docker logs pinot-broker > ${PN2_LOG_FILE} 2>&1
echo -e "[$(getNow)] pinot-broker Start. log diretory is ${PN2_LOG_FILE} \n"


# pinot-server start
docker start pinot-server
sudo docker logs pinot-server > ${PN3_LOG_FILE} 2>&1
echo -e "[$(getNow)] pinot-server Start. log diretory is ${PN3_LOG_FILE} \n"


# superset start
docker start superset 
sudo docker logs superset > ${SP_LOG_FILE} 2>&1
echo -e "[$(getNow)] superset Start. log diretory is ${SP_LOG_FILE} \n"


# get my PUBLIC_IP
MY_PUBLIC_IP=$(curl -s ifconfig.me)

# change kafka bootstrap-server address
sed -i "/advertised.listeners/ c\advertised.listeners=PLAINTEXT://${MY_PUBLIC_IP}:9092" ~/kafka_2.12-2.7.1/config/server.properties 
echo -e "[$(getNow)] change kafka bootstrap-server address ${MY_PUBLIC_IP}:9092\n"


# kafka start
# 1. kafka meta.properties remove
rm /tmp/kafka-logs/meta.properties

# 2. kafka server start
/home/ec2-user/kafka_2.12-2.7.1/bin/kafka-server-start.sh /home/ec2-user/kafka_2.12-2.7.1/config/server.properties >> ${KFK_LOG_FILE} 2>&1 &

echo -e "[$(getNow)] kafka start. log directory is ${KFK_LOG_FILE} \n"
sleep 10



# druid start
export DRUID_SKIP_JAVA_CHECK=1
export DRUID_SKIP_PORT_CHECK=1
/home/ec2-user/apache-druid-0.22.0/bin/start-nano-quickstart >> ${DRD_LOG_FILE} 2>&1 &
echo -e "[$(getNow)] Druid start. log directory is ${KFK_LOG_FILE} \n"
sleep 10

# fake data generator start
source /home/ec2-user/my_app/env/bin/activate
sleep 10
nohup python3 /home/ec2-user/py_script/fake_user_generator.py &
echo -e "[$(getNow)] fake user data generator.py start  \n"


# env on program end 
echo -e "[$(getNow)] ${RUN_FILE_NAME} End. \n"
