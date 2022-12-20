import time
from datetime import datetime
import json
import threading
import paramiko
from elasticsearch import Elasticsearch


def lag_pipeline(ssh_key, es_conn, hostname, topic_name, monitor_cli, key_list, run_event):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect( hostname=hostname, username='ec2-user', pkey=ssh_key )
    stdin, stdout, stderr = client.exec_command(monitor_cli)
    while run_event.is_set():
        line = stdout.readline()
        if not line:
            break
        info = [datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]+'Z',topic_name]
        lag = line.split('\n')[0].split(' ')
        dict1 = dict(zip(key_list, info))   
        dict2 = {'p'+str(i) : lag[i] for i in range(len(lag))}
        document = dict(dict1, **dict2)
        es_conn.index(index='consumer_lag', body= document)
        
 

def main():

    ## set ssh key variable for es connecting
    key_path = 'C:/Users/cloudpc/aws_pem/ec2_keypair_dev.pem'
    ssh_key = paramiko.RSAKey.from_private_key_file(key_path)
    es_address = ['input es ip address']
    es = Elasticsearch(es_address, basic_auth=('oasis','oasis1'))
    key_list = ['@timestamp','topic']


   
    ## create index 
    if es.indices.exists(index='consumer_lag') :
        pass
    else : 
        with open('C:/Users/cloudpc/Desktop/task/test_es/mapping.json', 'r') as f :
            mapping = json.load(f)
        es.indices.create (index='consumer_lag', body=mapping)



    ## insert lag monitoring value into elasticsearch by threading 
    print("lag monitoring program start")
    run_event = threading.Event()
    run_event.set()
    


    t2_cli = "while sleep 1; do echo -e $(/home/ec2-user/kafka_2.12-2.6.2/bin/kafka-consumer-groups.sh --bootstrap-server bootstrap_server_ip --group consumer group name --describe --command-config /home/ec2-user/kafka_2.12-2.6.2/config/mm-consumer.properties --describe 2> /dev/null | grep 'topic' | sed 's/\s\+/\t/g' | cut -f 6 | xargs); done"
    t3_cli = "while sleep 1; do echo -e $(/home/ec2-user/kafka_2.12-2.6.2/bin/kafka-consumer-groups.sh --bootstrap-server bootstrap_server_ip --group consumer group name --describe --command-config /home/ec2-user/kafka_2.12-2.6.2/config/mm-consumer.properties --describe 2> /dev/null | grep 'topic' | sed 's/\s\+/\t/g' | cut -f 6 | xargs); done"
    t4_cli = "while sleep 1; do echo -e $(/home/ec2-user/kafka_2.12-2.6.2/bin/kafka-consumer-groups.sh --bootstrap-server bootstrap_server_ip --group consumer group name --describe --command-config /home/ec2-user/kafka_2.12-2.6.2/config/mm-consumer.properties --describe 2> /dev/null | grep 'topic' | sed 's/\s\+/\t/g' | cut -f 6 | xargs); done"
    t1_cli = "while sleep 1; do echo -e $(/home/ec2-user/kafka_2.12-2.6.2/bin/kafka-consumer-groups.sh --bootstrap-server bootstrap_server_ip --group consumer group name --describe --command-config /home/ec2-user/kafka_2.12-2.6.2/config/mm-consumer.properties --describe 2> /dev/null | grep 'topic' | sed 's/\s\+/\t/g' | cut -f 6 | xargs); done"
    t5_cli = "while sleep 1; do echo -e $(/home/ec2-user/kafka_2.12-2.6.2/bin/kafka-consumer-groups.sh --bootstrap-server bootstrap_server_ip --group consumer group name --describe --command-config /home/ec2-user/kafka_2.12-2.6.2/config/mm-consumer.properties --describe 2> /dev/null | grep 'topic' | sed 's/\s\+/\t/g' | cut -f 6 | xargs); done"



    t1 = threading.Thread(target = lag_pipeline, args = (ssh_key, es, 'SSH IP', 'topic', t1_cli, key_list, run_event))
    t2 = threading.Thread(target = lag_pipeline, args = (ssh_key, es, 'SSH IP', 'topic', t2_cli, key_list, run_event))
    t3 = threading.Thread(target = lag_pipeline, args = (ssh_key, es, 'SSH IP', 'topic', t3_cli, key_list, run_event))
    t4 = threading.Thread(target = lag_pipeline, args = (ssh_key, es, 'SSH IP', 'topic', t4_cli, key_list, run_event))
    t5 = threading.Thread(target = lag_pipeline, args = (ssh_key, es, 'SSH IP', 'topic', t5_cli, key_list, run_event))
    t1.start()
    time.sleep(.5)
    t2.start()
    time.sleep(.5)
    t3.start()
    time.sleep(.5)
    t4.start()
    time.sleep(.5)
    t5.start()

    try:
        while 1:
            time.sleep(.1)
    except KeyboardInterrupt:
        print ("attempting to close threads.")
        run_event.clear()
        t1.join()
        t2.join()
        t3.join()
        t4.join()
        t5.join()
        print ("threads successfully closed")



if __name__ == '__main__':
    main()