from kafka import KafkaProducer
from json import dumps
import time


producer = KafkaProducer(acks=0, compression_type ='gzip', bootstrap_servers=['54.180.103.204 :9092'],
                         value_serializer=lambda x:dumps(x).encode('utf-8'))
                         
                         
start = time.time()

for i in range(50):
    data = {'str' : 'result'+str(i)}
    producer.send('test2', value=data)
    producer.flush()
    
    print("elapesd:", time.time()-start)