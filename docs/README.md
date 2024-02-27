# Architecture Overview

## Sunbird Architecture
Refer to [Sunbird Tech-Architecture pdf](Sunbird%20-%20Tech%20Architecture%20.pdf)

## iGoT data-pipeline architecture
Refer to [Sunbird Tech-Architecture pdf](Sunbird%20-%20Tech%20Architecture%20.pdf)

## Why Sunbird?
Why are we using Sunbird for iGoT project?

- Microservices architecture - enables decoupling and allows various teams to collaborate on different features of the platform simultaneously
- Scaling - each of the services can be horizontally scaled separately 
- Open source
- Employs Cutting edge open-source technologies
- Proven and battle-tested

## Motivation Behind data-pipeline architecture
Motivation behind data-pipeline architecture - what are we trying to achieve and how does this architecture enable it?

- Need for real-time usage data is enabled by stream processing, enabling real-time data to be displayed on dashboards
- Need for exhaustive reporting and dashboards, while interacting with multiple data-sources is enabled by batch processing
- Architecture is designed to be decoupled with multiple microservices, each of which can be horizontally scaled separately

## Intro to data-pipeline technologies
- Telemetry Service - iGOT service responsible for producing telemetry events to Kafka
- Apache Kafka - Transport for async inter-service communication
- Apache Flink - Distributed stream processing framework
- Apache Spark - Distributed batch processing framework
- Secor - library by Pinterest for efficient and robust Kafka topic backups
- Apache Druid - OLAP store for ad-hoc analytical queries
- RAIN - Tarento's in-house accelerator, provides functionality for querying multiple data-sources and data-visualizations
- Monitoring Technologies - Prometheus, Grafana, etc.
- DevOps Technologies - Jenkins, Ansible, Kubernetes, Helm


# Telemetry

Telemetry enables automatic collection of data from real-world, real-time use.

- Event driven architecture - Instead of having to query application data-sources for data, we have the services emmit data in real-time
- Front end telemetry events are emitted by frontend services when a user action is performed
- Back end telemetry events are emitted by backend services when a particular event occurs during backend processing
- Sunbird provides a Standalone JS Library as well as an HTML Interface Library to simplify telemetry generation
- Both of the above call Telemetry Service APIs which can also be called directly

## Sunbird Telemetry Event Structure
```javascript

{
  // About the event
  "eid": , // Required.
  "ets": , // Required. Epoch timestamp of event (time in milli-seconds. For ex: 1442816723)
  "ver": , // Required. Version of the event data structure, currently "3.0"
  "mid": , // Required. Unique message ID. Used for deduplication, replay and update indexes

  // Who did the event
  "actor": { // Required. Actor of the event.
    "id": , // Required. Can be blank. Id of the actor. For ex: uid incase of an user
    "type":  // Required. Can be blank. User, System etc.
  },

  // Context of the event
  "context": { // Required. Context in which the event has occured.
    "channel": , // Required. Channel which has produced the event
    "pdata": { // Optional. Producer of the event
      "id": , // Required. unique id assigned to that component
      "pid": , // Optional. In case the component is distributed, then which instance of that component
      "ver":  // Optional. version number of the build
    },
    "env": , // Required. Unique environment where the event has occured.
    "sid": , // Optional. session id of the requestor stamped by portal
    "did": , // Optional. uuid of the device, created during app installation
    "cdata": [{ // Optional. correlation data
      "type":"", // Required. Used to indicate action that is being correlated
      "id": "" // Required. The correlation ID value
    }],
    "rollup": { // Optional. Context rollups example: Organization heirarchy("L1": "Parent org id", "l2": "sub org id")
      "l1": "",
      "l2": "",
      "l3": "",
      "l4": ""
    }
  },

  // What is the target of the event
  "object": { // Optional. Object which is the subject of the event.
    "id": , // Required. Id of the object. For ex: content id incase of content
    "type": , // Required. Type of the object. For ex: "Content", "Community", "User" etc.
    "ver": , // Optional. version of the object
    "rollup": { // Optional. Rollups to be computed of the object. Only 4 levels are allowed. example: Textbook heirarchy("L1": "Parent node id", "l2": "Child node id")
      "l1": "",
      "l2": "",
      "l3": "",
      "l4": ""
    }
  },

  // What is the event data
  "edata": {} // Required.

  // Tags
  "tags": [""] // Optional. Encrypted dimension tags passed by respective channels
}

```

## Common Telemetry Events

Front-end events
* START - Start of a multi step activity. For ex: Session, App, Tools, Community etc would have start and end
* END - End of a work flow
* IMPRESSION - A visit to a specific page by an user
* INTERACT - User interaction on the page (such as search, click, preview, move, resize, configure)

Back-end events
* AUDIT - When an object is changed. This includes lifecycle changes as well.
* LOG - Generic log event. API call, Service call, app update etc
* SEARCH - A search is triggered (content, item, asset etc.)
* SUMMARY - A summary event

[Full telemetry spec](telemetry-v3.md)

## Implementing new Telemetry events
- Based on the telemetry specification decide if the new event type fits in with the existing defined telemetry types
- If a new telemetry event type is needed, provisions for its processing would need to be made
  - New specification must be defined - [example](../data-pipeline-flink/pipeline-preprocessor/src/main/resources/schemas/telemetry/3.0/cb_audit.json)
  - New kafka topics would need to be created and data-pipeline components would need to be modified to accommodate the new event's processing
- Any schema changes required to validate the events must be made [here](../data-pipeline-flink/pipeline-preprocessor/src/main/resources/schemas/telemetry/3.0)
- Create clear document on what each of the field's allowed values are, and what are the expected values are in different situations/portals
- Communicate with the concerned external team


# Kafka

## What is a message queue?
A message queue is a form of asynchronous service-to-service communication used in serverless and microservices
architectures. Messages are stored on the queue until they are processed and deleted

Examples:- RabbitMQ, AWS Simple Queue Service (SQS), Kafka


## Introduction to Apache Kafka

https://kafka.apache.org/28/documentation.html

Apache Kafka is a distributed data streaming platform that can publish, subscribe to, store, and process streams of
records in real time

Can act as a -
- Message queue - at-least-once delivery, which means that each message is delivered at least once
- FIFO queue - exactly-once processing, which means that each message is delivered once and remains available until a consumer processes it and deletes it
- Pub/Sub messaging system - the publisher sends messages to a topic, and multiple subscribers can listen to the topic
- Real-time streaming platform
- Kafka can also be used as a database due to its durable storage capability

### Architecture -

![kafka architecture](images/apache-kafka-architecture3.png)

#### Kafka Cluster
A Kafka cluster is a system that consists of different brokers, topics, and their respective partitions. Data is written
to the topic within the cluster and read by the cluster itself.

#### Producers
A producer sends or writes data/messages to the topic within the cluster. In order to store a huge amount of data,
different producers within an application send data to the Kafka cluster.

#### Consumers
A consumer is the one that reads or consumes messages from the Kafka cluster. There can be several consumers consuming
different types of data form the cluster. Each consumer knows from where it needs to consume the data.

#### Brokers
A Kafka server is known as a broker. A broker is a bridge between producers and consumers

#### Topics
Kafka topics are the categories used to organize messages. Each topic has a name that is unique across the entire Kafka cluster.
Messages are sent to and read from specific topics
- Partitions - divides a singe topic log into multiple ones, each of which can live on a separate node in the Kafka cluster
- Replication factor - How many replicas of each partition exist on the cluster
- Retention - The period till which kafka must store the messages in the topic (from time of production)
- Offsets - it is a position within a partition for the next message to be sent to a consumer

#### ZooKeeper
A ZooKeeper is a service used to store information about the Kafka cluster and details of the consumer clients.
- It manages brokers by maintaining a list of them. Also, a ZooKeeper is responsible for choosing a leader for the partitions.
- If any changes like a broker die, new topics, etc., occurs, the ZooKeeper sends notifications to Apache Kafka.
- A user does not directly interact with the Zookeeper, but via brokers. No Kafka server can run without a zookeeper server.

## Kafka in iGOT
We have 2 kafka clusters running on iGOT
- KP Kafka - for Knowledge Platform services - ingestion-cluster
- DP Kafka - for Data Pipeline services - processing-cluster

### DP Kafka

Ansible role - [setup-kafka](../ansible/roles/setup-kafka)

[Default topic configurations](../ansible/roles/setup-kafka/defaults/main.yml)

#### Important Topics in DP kafka

- {env}.telemetry.ingest - raw batch events appear here
- {env}.telemetry.raw - raw events
- {env}.telemetry.unique - unique events
- {env}.telemetry.derived - summary events
- {env}.druid.events.telemetry - druid telemetry events
- {env}.druid.events.summary - druid summary events

## Basic commands for Kafka

```shell
# Start the ZooKeeper service
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start the Kafka broker service
bin/kafka-server-start.sh config/server.properties

# to create a topic
/opt/kafka/bin/kafka-topics.sh --create --topic <topic> --replication-factor 1 --partitions 1 --bootstrap-server localhost:9092

# to create topic for older version of kafka
/opt/kafka/bin/kafka-topics.sh --create --topic <topic> --replication-factor 1 --partitions 1 --zookeeper localhost:2181

# to list topics
/opt/kafka/bin/kafka-topics.sh --list --zookeeper localhost:2181

# get topic details
./bin/kafka-topics.sh --zookeeper localhost:2181 --describe --topic prod.telemetry.raw

# console consumer: start consumer add `--from-beginning` to get all messages from start
/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic <topic>

# console producer
/opt/kafka/bin/kafka-console-producer.sh --topic <topic> --broker-list localhost:9092

# modify a topic
/opt/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --alter --topic <topic> --config retention.ms=172800000
/opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type topics --entity-name <topic> --alter --add-config retention.ms=172800000

# empty a topic
/opt/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --alter --topic <topic> --config retention.ms=0
/opt/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --alter --topic <topic> --config retention.ms=172800000

# Delete a topic
bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic <topic>

# get offsets for a topic
/opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic <topic>

# get offsets for a consumer-group
/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group <group> --describe

```

# Flink

## Stream Processing

Stream processing is a computing paradigm used to process and analyze continuous streams of data in real-time or near-real-time.
Instead of storing data and processing it later, stream processing systems handle data as it arrives, making them suitable
for applications that require immediate insights or actions based on incoming data.

- Real-time Data Processing
- Parallel Processing
- Event-Driven Architecture
- Low Latency
- Scalability (horizontal)

### Use Cases
Anything that requires real-time Analytics on data streams from various sources, such as sensors, social media feeds, click-stream data, logs, and financial transactions
- Fraud Detection: to analyze transactions, identify suspicious activities, and trigger alerts or actions in real-time
- Internet of Things (IoT) Applications: real-time analysis of sensor data for applications like predictive maintenance, smart cities, and environmental monitoring.
- Real-time Monitoring and Alerting: real-time monitoring of system performance, network traffic, application logs, and security events.
- Recommendation Systems: (Netflix, Youtube, Amazon)

### Why Flink?

- Exactly-Once Semantics
  - Flink ensures each event is processed and results are computed reliably, without duplication or loss.
  - This level of reliability is critical for many real-time applications, such as financial transactions and fraud detection.

- Stateful Stream Processing
  - Flink natively supports stateful stream processing, allowing applications to maintain and update state across event streams.
  - This is essential for complex computations and analytics that require contextual information or aggregation over time windows.

- Advanced Windowing and Time Handling
  - Flink provides various types of time windows, such as tumbling, sliding, and session windows to aggregate data over time intervals
  - Beneficial for time-based analysis and aggregation tasks.

- High Performance and Scalability
  - Support for parallel processing and distributed execution
  - Can efficiently handle large volumes of data and scale horizontally to accommodate growing workloads by leveraging features like task parallelism and data partitioning.


## Flink Intro
Basic concepts, checkpointing, only once, stream tasks, process functions, windows, watermarking etc.

- DataStream and DataSet
  - DataStream represents a stream of data elements that are continuously processed in real-time
  - DataSet represents a static, bounded collection of data elements that are processed in batch mode

- Transformation
  - Transformations are operations applied to DataStreams or DataSets to modify, filter, aggregate, or analyze data
  - map, filter, reduce, join, and window, enabling developers to manipulate data streams and datasets efficiently.

- Windowing
  - partition data streams into finite, non-overlapping or overlapping time-based segments called windows
  - Flink supports various types of windows, including tumbling windows, sliding windows, and session windows

- Stateful Processing
  - Allows applications to maintain and update state across event streams.
  - State can be used to store intermediate results, maintain session information, or perform aggregations over time windows
  - Flink provides built-in mechanisms for managing state, including keyed state and operator state.
  
- Checkpointing and Fault Tolerance
  - Checkpointing is a mechanism in Flink for ensuring fault tolerance and exactly-once processing semantics
  - Flink periodically takes snapshots of the application state and metadata, which can be used to restore the application's state in case of failures
  - Checkpointing is essential for maintaining data consistency and reliability in distributed stream processing applications

- Event Time and Processing Time
  - Event time refers to the time at which events occur in the real world
  - Processing time refers to the time at which events are processed by the system
  - Flink supports event time processing for accurate and consistent results, even in the presence of delayed or out-of-order events.

- Watermarks
  - Watermarks are markers emitted by Flink to indicate progress in event time
  - They are used to track the completeness of event streams and to trigger window computations
  - Watermarks help Flink handle out-of-order events and late data by providing a notion of progress in event time.

- Execution Environment
  - Local execution for development and testing
  - Standalone clusters
  - Apache YARN
  - Apache Mesos
  - Kubernetes

## Hello World

Installation and local run:

https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/try-flink/local_installation/


## iGoT data-pipeline Flink jobs

![iGoT Stream processing architecture](images/Stream%20processing%20architecture.drawio.svg)

## Understanding iGoT Flink jobs
- [Intro to dp-core](flink-dp-3.6.md)
- Example code Walkthrough - How a Flink project is structured/coded in iGoT Data-pipeline
- Local Build -
  - `cd data-pipeline-flink`
  - `mvn3.6 clean install -DskipTests`
  - `cd sunbird-dp-distribution`
  - `mvn3.6 package -Pbuild-docker-image -Drelease-version=<version>`
- to only build one of the modules
  - `mvn3.6 clean install -pl <specific-project> -am -DskipTests`

## DevOps
- Configurations - [values.j2](../kubernetes/helm_charts/datapipeline_jobs/values.j2)
- Build - [check file - kubernetes/pipelines/build/Jenkinsfile](../kubernetes/pipelines/build/Jenkinsfile)
- Deployment - [check file - kubernetes/pipelines/deploy/flink-jobs/Jenkinsfile](../kubernetes/pipelines/deploy/flink-jobs/Jenkinsfile)

## Common Operations
```sh
# ssh to kubernetes server, for pre-prod
# ssh admin-192.168.3.215@10.194.181.118

# list pods flink-dev namespace
kubectl get po -n flink-dev  # flink-prod for prod and BM

# get logs for a pod
kubectl logs <pod> -n flink-dev  # add -f to follow the logs

# get configurations
kubectl get cm -n flink-dev 

# view/edit configurations
kubectl edit cm <config-name> -n flink-dev

# to restart a pod
kubectl delete po <pod-name> -n flink-dev

# restart all pods in the namespace
kubectl delete po --all -n flink-dev

# to remove a deployment completely
kubectl delete deploy <deployment-name> -n flink-dev

# to remove a job completely
kubectl delete jobs <job-name> -n flink-dev
```

### Check lag, reset offset

First obtain the consumer group id and kafka cluster ip from config, then

```sh
# ssh to kafka cluster
# get offsets for a consumer-group
/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group <group> --describe

# reset offset
/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group <group> --reset-offsets --to-earliest --topic <topic> -execute
/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group <group> --reset-offsets --to-latest --topic <topic> -execute

/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group <group> --reset-offsets --to-datetime 2020-12-20T00:00:00.000 --topic <topic> --execute

# other options
# --shift-by <positive_or_negative_integer>
# --to-current
# --to-latest
# --to-offset <offset_integer>
# --to-datetime <datetime_string>
# --by-duration <duration_string>
```



### Common issues
- One or more pods are not present
  - maybe an intermittent error due to some service shutting down for a small period (redis, cassandra, kafka etc.)
    - re-deploy the last image
  - if pod is getting repeatedly restarted
    - check logs to see what the error is
    - kafka error - broker not available, wrong cluster is configured, topic missing, kafka VM disk space is full
    - validation error - faulty message is present (or some service is still producing them), reset offset shift by
    - snappy error - a snappy compressed message is in some topic
    - checkpointing error - ceph could be down