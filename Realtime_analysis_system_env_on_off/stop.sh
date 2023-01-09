#!/usr/bin/env bash

getNow()
{
        echo "`date '+%Y-%m-%d %H:%M:%S'`"
}

getNowTimestamp()
{
		echo "`date '+%s'`"
}


# env off program start 
echo -e "[$(getNow)] ${RUN_FILE_NAME} Start. \n"


# python generator stop 
kill -9 `ps -ef |grep python |grep -v grep | awk '{print $2}'`
echo -e "[$(getNow)] python generator stop. \n"

# docker stop
echo -e "[$(getNow)] docker process stop. \n"
docker stop $(docker ps -qa)


# druid stop
/home/ec2-user/apache-druid-0.22.0/bin/service --down
echo -e "[$(getNow)] druid process stop. \n"


# kafka stop 
kill -9 `ps -ef |grep Kafka |grep -v grep | awk '{print $2}'`
echo -e "[$(getNow)] kafka process stop. \n"


# env off program end
echo -e "[$(getNow)] ${RUN_FILE_NAME} end. \n"
