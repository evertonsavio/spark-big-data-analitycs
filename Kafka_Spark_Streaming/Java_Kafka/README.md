* Download Binary files of Kafka and extract on C: for example  
  
* Dentro da pasta extraida acesse bin/windows  
  
* No terminal inicie o zookeeper e depois o kafka  
```.\zookeeper-server-start.bat ..\..\config\zookeeper.properties```   
```.\kafka-server-start.bat ..\..\config\server.properties```     
* Criar topico chamado viewrecords      
```  
.\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic viewrecords  
```  
  
* Verificar topicos:   
``` .\kafka-topics.bat --list --zookeeper localhost:2181 ```  
  
* Verficar consumer:  
``` .\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic viewrecords ```