<h1 align="center">Demo application using apache-flink + kafka + docker </h1>                   

We have created these repo to run apache flink jobs for computation on data produced by kafka in a dockerised environment. 

**Tech stack :** 

**Writing jobs : Pyflink**

**python-version : 3.10.12**

**apache-flink ==1.17.1**



## Docker should be running before writing the below commands :

 *docker-compose build*  
 *docker-compose up -d* 

## For submission of job : 
*docker exec -it demo-jobmanager-1 flink run -py /opt/flink/usrlib/kafka_processing.py*

Now start your kafka producer and consumer in another shell :

**Producer :**
*docker exec -it demo-kafka-1 kafka-console-producer.sh --broker-list kafka:9092 --topic input-topic*

These are some of messages you try 

> {"user_id": "user7", "item_id": "item15", "category_id": "home", "behavior": "purchase", "ts": "2023-02-26 11:23:45"}
> {"user_id": "user8", "item_id": "item22", "category_id": "fashion", "behavior": "wishlist", "ts": "2023-02-26 11:42:10"}
> {"user_id": "user6", "item_id": "item8", "category_id": "toys", "behavior": "add_to_cart", "ts": "2023-02-26 11:12:30"}

**Consumer :**  
*docker exec -it demo-kafka-1 kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic output-topic --from-beginning*

Output expected :

{"user_id":"user7","item_id":"item15","category_id":"home","behavior":"purchase","ts":"2023-02-26 11:23:45","processing_time":"2025-02-26 14:17:33.238"}
{"user_id":"user8","item_id":"item22","category_id":"fashion","behavior":"wishlist","ts":"2023-02-26 11:42:10","processing_time":"2025-02-26 14:17:52.234"}
{"user_id":"user6","item_id":"item8","category_id":"toys","behavior":"add_to_cart","ts":"2023-02-26 11:12:30","processing_time":"2025-02-26 14:18:09.983"}
