from datetime import datetime
from elasticsearch import Elasticsearch
import numpy as np
import base64
import tensorflow as tf
import cv2
import pika


#Model loading
model_path = "/opt/airflow/model/model.h5"
model = tf.keras.models.load_model(model_path)

#RabbitMQ Connection
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rabbitmq'))
channel = connection.channel()

channel.queue_declare(queue='base64')
channel.queue_declare(queue='prediction')


#ElastiSearch Connection
es = Elasticsearch([{'host': 'elasticsearch'}])

#Query to fetch last mins data
body={
  "query": {
    "match_all": {}
  },
  "size": 20,
  "sort": [
    {
      "@timestamp": {
        "order": "desc"
      }
    }
  ]
}

result = es.search(index="filebeat-7.9.2-2022.09.07-000001",body=body)
response = result["hits"]["hits"]

#For each hit
for doc in response:
    #Convert Base64 to JPG
    code64 = doc["_source"]["message"]
    code64_bytes = code64.encode('utf-8')
    f = open("/opt/airflow/converted.jpg", "wb")
    decoded_image_data = base64.decodebytes(code64_bytes)
    f.write(decoded_image_data)
    f.close()
    #Read the JPG
    img = cv2.imread("/opt/airflow/converted.jpg")
    img = cv2.resize(img, (48, 48))
    img = np.expand_dims(img, axis=0)
    #Results
    res = np.argmax(model.predict(img), axis=1)
    print(res[0])
    #Send to RabbitMQ
    channel.basic_publish(exchange='', routing_key='base64', body=code64)
    channel.basic_publish(exchange='', routing_key='prediction', body=str(res[0]))
    
connection.close()
