<p align="center">
    <a href="https://cloud.ibm.com">
        <img src="https://landscape.cncf.io/logos/ibm-cloud-kcsp.svg" height="100" alt="IBM Cloud">
    </a>
</p>

<p align="center">
    <a href="https://cloud.ibm.com">
    <img src="https://img.shields.io/badge/IBM%20Cloud-powered-blue.svg" alt="IBM Cloud">
    </a>
    <a href="https://www.ibm.com/developerworks/learn/java/">
    <img src="https://img.shields.io/badge/platform-java-lightgrey.svg?style=flat" alt="platform">
    </a>
    <img src="https://img.shields.io/badge/license-Apache2-blue.svg?style=flat" alt="Apache 2">
</p>


# Java Spring microservice

In this sample application, you will create a basic Java web application using Spring. This provides a starting point for creating Java microservice applications running on [Spring](https://spring.io/). It contains no default application code, but comes with standard best practices, including a health check and application metric monitoring.

Capabilities are provided through dependencies in the `pom.xml` file. The ports are set to the defaults of `8080` for http and `8443` for https and are exposed to the CLI in the `cli-config.yml` file. The ports are set in the `pom.xml` file and exposed to the CLI in the `cli-config.yml` file.

## Steps

You can [deploy this application to IBM Cloud](https://cloud.ibm.com/developer/appservice/create-app?starterKit=1298bc4e-4764-390b-a9eb-e4dcf3cc03ad) or [build it locally](#building-locally) by cloning this repo first. Once your app is live, you can access the `/health` endpoint to build out your cloud native application.

### Deploying 

After you have created a new git repo from this git template, remember to rename the project.
Edit `package.json` and change the default name to the name you used to create the template.

Make sure you are logged into the IBM Cloud using the IBM Cloud CLI and have access 
to you development cluster. If you are using OpenShift make sure you have logged into OpenShift CLI on the command line.

```$bash
npm i -g @garage-catalyst/ibm-garage-cloud-cli
```

Use the IBM Garage for Cloud CLI to register the GIT Repo with Jenkins 
```$bash
igc pipeline -n dev
```

### Building Locally

To get started building this application locally, you can either run the application natively or use the [IBM Cloud Developer Tools](https://cloud.ibm.com/docs/cli?topic=cloud-cli-getting-started) for containerization and easy deployment to IBM Cloud.

#### Native Application Development

* [Maven](https://maven.apache.org/install.html)
* Java 11: Any compliant JVM should work.
  * [Java 11 JDK from Oracle](http://www.oracle.com/technetwork/java/javase/downloads/index.html)
    or [Download a Liberty server package](https://developer.ibm.com/assets/wasdev/#filter/assetTypeFilters=PRODUCT)
    that contains the IBM JDK (Windows, Linux)
    
To build and run an application:
1. `./gradlew build`
2. `./gradlew bootRun`


## More Details

For more details on how to use this Starter Kit Template please review the [IBM Garage for Cloud Developer Tools Developer Guide](https://ibm-garage-cloud.github.io/ibm-garage-developer-guide/)

## Next Steps
* Learn more about augmenting your Java applications on IBM Cloud with the [Java Programming Guide](https://cloud.ibm.com/docs/java?topic=java-getting-started).
* Explore other [sample applications](https://cloud.ibm.com/developer/appservice/starter-kits) on IBM Cloud.

## License

This sample application is licensed under the Apache License, Version 2. Separate third-party code objects invoked within this code pattern are licensed by their respective providers pursuant to their own separate licenses. Contributions are subject to the [Developer Certificate of Origin, Version 1.1](https://developercertificate.org/) and the [Apache License, Version 2](https://www.apache.org/licenses/LICENSE-2.0.txt).

[Apache License FAQ](https://www.apache.org/foundation/license-faq.html#WhatDoesItMEAN)

### Periodically update from the template

Finally, the template components can be periodically updated by running the following:

```bash
./update-template.sh
```

## Kafka setup on local laptop

Intall Kafka

Start Zookeeper

```bash
./bin/zookeeper-server-start.sh config/zookeeper.properties
```

Start Kafka server/broker
```bash
./bin/kafka-server-start.sh config/server.properties
```

## Useful Kafka commands

Describe Kafka topics
```bash
./bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe
```

List Kafka topics
```bash
./bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Delete Kafka topic
```bash
./bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic demo
```

## Simple Kafka example 
This app consists of a simple kafka producer rest endpoint that can write a message to a kafka topic and a simple consumer rest endpoint that reads a message from the topic.
See the code in java package 'com.ibm.simplekafka'.

Create a Kafka topic called demo with 1 partition and replication factor of 1
```bash
./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic demo
```

## Orders Kafka example 
This app consists of a a producer rest endpoint that sends an order to a kafka topic.  
There is an listener agent that listens for any new messages/orders on the topic and handles them.
Right now, the handler consists of logging the order message.

For the orders demo to work, the following topic needs to be created:
```bash
./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic orders
```

## Word Count Streaming Kafka example 

This app consists of a producer rest endpoint that sends a string of words to an input topic.
There is a listener agent running the word count stream application that listens for new messages on the input topic
and counts the occurance of individual words and puts the result on the output topic.

For the word count stream app to work, the following topics needs to be created:
```bash
./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic streams-wordcount-plaintext-input
```
```bash
./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic streams-wordcount-output
```

To monitor the output, you need to run the following command in a terminal window:
```bash
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic streams-wordcount-output \
    --from-beginning \
    --formatter kafka.tools.DefaultMessageFormatter \
    --property print.key=true \
    --property print.value=true \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
```

## Pipe Streaming Kafka example 

The pipe stream app basically takes the value on the input topic and puts it on the output topic.
For the pipe stream app to work, the following topics needs to be created:

```bash
./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic streams-pipe-input
```
```bash
./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic streams-pipe-output
```

Start a kafka producer and consumer as follows:

```bash
./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic streams-pipe-input
```
```bash
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic streams-pipe-output --from-beginning
```

## Mapping Stream Kafka example

The mapping stream app reverses the key value pair so that on record on the output topic has the value as the key and the key as the value.  


For the mapping stream app to work, the following topics needs to be created:

```bash
./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic mapping-stream-input
```
```bash
./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic mapping-stream-output
```
 
 Start a kafka producer and consumer as follows:   

```bash
./bin/kafka-console-producer.sh --broker-list localhost:9092     --topic mapping-stream-input     --property "parse.key=true"     --property "key.separator=:"
```

```bash
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092     --topic mapping-stream-output     --from-beginning     --formatter kafka.tools.DefaultMessageFormatter     --property print.key=true     --property print.value=true     --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer     --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
```    
    
