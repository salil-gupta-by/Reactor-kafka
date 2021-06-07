# Reactor-kafka

Installations-
1) Kafka on your local

Run Application in Windows-
1) Run Zookeeper and kafka in separate cmd windows using below commands-

zookeeper-server-start.bat config\zookeeper.properties
kafka-server-start.bat config\server.properties

2) Create topic app_updates using below command in a separate cmd window.
kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic employee_updates
kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic employee_DLQ

3) Open Kafka Consumer and producer consoles in separate windows using below commands-
kafka-console-producer.bat --broker-list localhost:9092 --topic app_updates
kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic app_updates

4)create a .bat file with java -jar UpdateApp-0.0.1-SNAPSHOT.jar and copy the .bat file in the same directory of the jar.
Double click to run the file.

5) Path for the jar is Reactor-kafka/target/UpdateApp-0.0.1-SNAPSHOT.jar

6) Application will run on localhost 8081

7) Kafka listener will be running and it will consume the Employee JSON String from the producer console and upon validation it will produce a message to topic employee_updates if valid and if not will push a message to employee_DLQ topic instead.
