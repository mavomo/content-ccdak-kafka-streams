
## Getting started

> docker exec -it -w /opt/kafka/bin broker sh

To get into the container and access the terminal

#### Starting a producer: 

1. Basic streams
> ./kafka-console-producer.sh  --bootstrap-server broker:29092 --topic streams-input-topic --property pars
e.key=true --property key.separator=:



#### Starting a consumer

1. Basic streams

> /kafka-console-consumer.sh --topic streams-output-topic --property print.key=true --from-beginning  --b
ootstrap-server broker:29092
