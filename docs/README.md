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


# Secor

Secor is an efficient kafka backup utility developed by Pinterest
- Secor process runs continuously and creates back-up files on local disk
- Secor uploads these local backups to blob storage after a configured amount of time has passed


```shell
# check logs on blob storage
# ssh to jenkins server (251 for prod)

# all backups
s3cmd ls s3://igotlogs/secor-prod/

# unique backups
s3cmd ls s3://igot/secor-prod/unique/raw/
```

```shell
# ssh to secor server
sudo su analytics

# start and stop the secor process
/home/analytics/sbin/secor unique-telemetry-backup stop
/home/analytics/sbin/secor unique-telemetry-backup start

# check logs
cd /mount/secor/logs
ls -ltrh
```

#### Repos:
https://github.com/sunbird-cb/secor/tree/secor-0.25 - secor-0.25 ====> to build and generate the artifacts.
https://github.com/sunbird-cb/sunbird-data-pipeline - release-3.7.0 ===> for deployment

https://github.com/project-sunbird/sunbird-devops - release-3.7.0 =====> for jenkins job.

### Secor Deploy
- Deploy - [Jenkinsfile.flink](../pipelines/deploy/secor/Jenkinsfile.flink)


# Spark, Data-products

## Spark Intro
Apache Spark is an open-source distributed computing system that is designed for big data processing and analytics.
It provides an interface for programming entire clusters with implicit data parallelism and fault tolerance.

- Performance: Spark's in-memory computation and optimization techniques often lead to faster processing times compared to other batch processing frameworks, especially for iterative algorithms or jobs requiring multiple passes over the data.
- Ease of Use: Spark provides high-level APIs in multiple languages like Java, Scala, Python, and R, which makes it easier for developers to write and maintain code. Additionally, Spark's rich ecosystem includes libraries for SQL, machine learning, graph processing, and streaming data, reducing the need for developers to reinvent the wheel.
- Unified Platform: Spark is a unified analytics engine, meaning it supports multiple workloads such as batch processing, interactive queries, streaming analytics, and machine learning within a single framework. This simplifies the architecture and reduces the need for managing multiple systems.
- Fault Tolerance: Spark's resilient distributed datasets (RDDs) provide built-in fault tolerance, allowing it to recover from failures gracefully without manual intervention. This is crucial for large-scale data processing where failures are common.
- Scalability: Spark is designed to scale from a single server to thousands of machines, making it suitable for processing large datasets. It can leverage distributed computing resources efficiently, providing linear scalability with the size of the cluster.
- Flexibility: Spark can run on various cluster managers like Apache Mesos, Hadoop YARN, and Kubernetes, giving users the flexibility to deploy it in different environments and integrate with existing infrastructure seamlessly.
- Community and Ecosystem: Spark has a vibrant open-source community and a rich ecosystem of tools and libraries built around it. This includes integration with popular data sources, connectors to databases, visualization tools, and third-party extensions, making it easier to solve complex data processing tasks.

## Spark Architecture

![Spark architecture](images/spark-architecture.png)

- Driver: The driver is the main entry point of a Spark application. It runs the user's main function and coordinates the execution of the Spark jobs. The driver communicates with the cluster manager to acquire resources and schedule tasks.
- Cluster Manager: Spark supports multiple cluster managers such as Apache Mesos, Hadoop YARN, and Kubernetes. The cluster manager allocates resources (CPU, memory) across the worker nodes in the cluster and manages their lifecycles. It's responsible for launching and monitoring the executors.
- Executors: Executors are worker nodes in the Spark cluster responsible for executing tasks and storing data in memory or disk. Each executor runs multiple tasks concurrently and communicates with the driver program. Executors cache data in memory to improve performance and provide fault tolerance through replication.
- Worker Nodes: Worker nodes host one or more executors and are responsible for executing tasks and storing data. They are managed by the cluster manager and can be added or removed dynamically based on the workload.
- RDD (Resilient Distributed Dataset): RDD is the fundamental data abstraction in Spark. It represents an immutable, distributed collection of objects partitioned across the worker nodes. RDDs support fault tolerance through lineage and can be rebuilt if a partition is lost.
- Data Partitioning: RDDs are divided into partitions, which are the basic units of parallelism in Spark. Partitions are distributed across the worker nodes, and tasks are executed in parallel on these partitions.
- Scheduler: Spark's scheduler determines how tasks are scheduled and executed across the worker nodes. It divides the job into stages and tasks and optimizes the execution plan based on data locality and task dependencies.
- DAG (Directed Acyclic Graph) Scheduler: The DAG scheduler breaks down the job into stages of tasks and constructs a DAG representing the data flow dependencies. It optimizes the execution plan by pipelining transformations and minimizing data shuffling.
- Task Execution: Tasks are the smallest units of work in Spark and are executed on the worker nodes. Tasks process data partitions and perform transformations or actions defined by the user. Spark supports pipelining tasks to minimize data movement and maximize parallelism.
- Caching and Persistence: Spark allows data to be cached in memory or disk across multiple operations, improving performance by avoiding recomputation. Caching can be controlled using RDD.persist() or DataFrame.cache().

### Other basic concepts

- Transformations: Transformations are operations that are applied to RDDs to create a new RDD. Examples include map, filter, reduceByKey, join, etc. Transformations are lazy, meaning they are not executed immediately but build up a computation plan (DAG) that is executed only when an action is called.
- Actions: Actions are operations that trigger the execution of the computation plan built by transformations and return results to the driver program or write data to an external storage system. Examples include collect, count, saveAsTextFile, etc.
- Spark Context: Spark Context is the main entry point for interacting with Spark. It represents the connection to a Spark cluster and is used to create RDDs, broadcast variables, and accumulators, as well as to set configuration parameters.
- SparkSession: SparkSession is the entry point for Spark SQL functionality. It allows you to interact with structured data using DataFrames and SQL queries. SparkSession encapsulates SparkContext, SQLContext, and HiveContext.
- DataFrames and Datasets: DataFrames and Datasets are higher-level abstractions introduced in Spark for working with structured data. They provide a more user-friendly API compared to RDDs and offer optimizations through the Catalyst query optimizer.
- Broadcast Variables and Accumulators: Broadcast variables allow efficient distribution of read-only data to all worker nodes, while accumulators are variables that can be added to by tasks and are typically used for aggregating results or collecting statistics.

## Hello World
https://spark.apache.org/docs/2.4.0/

## iGoT data-pipeline Spark jobs

![iGoT Batch processing architecture](images/batch-processing-architecture.drawio.svg)

### Configuration
![Model config](../ansible/roles/data-products-deploy/templates/model-config.j2)

WFS
```json
{
  "search": {
    "type": "{{ dp_object_store_type }}",
    "queries": [
      {
        "bucket": "'$bucket'",
        "prefix": "{{ dp_raw_telemetry_backup_location }}",
        "endDate": "'$endDate'",
        "delta": 0
      }
    ]
  },
  "model": "org.ekstep.analytics.model.WorkflowSummary",
  "modelParams": {
    "storageKeyConfig": "{{ dp_storage_key_config }}",
    "storageSecretConfig": "{{ dp_storage_secret_config }}",
    "apiVersion": "v2",
    "parallelization": 200
  },
  "output": [
    {
      "to": "kafka",
      "params": {
        "brokerList": "'$brokerList'",
        "topic": "'$topic'",
        "compression": "{{ dashboards_broker_compression }}"
      }
    }
  ],
  "parallelization": 200,
  "appName": "Workflow Summarizer",
  "deviceMapping": true
}
```

Dashboard Jobs
```json
{
  "search": {
    "type": "none"
  },
  "model": "org.ekstep.analytics.dashboard.report.acbp.UserACBPReportJob",
  "modelParams": {
    "debug": "false",
    "validation": "false",
    "redisHost": "{{ dashboards_redis_host }}",
    "redisPort": "{{ dashboards_redis_port }}",
    "redisDB": "{{ dashboards_redis_db }}",
    "sparkCassandraConnectionHost": "{{ core_cassandra_host }}",
    "sparkDruidRouterHost": "{{ druid_router_host }}",
    "sparkElasticsearchConnectionHost": "{{ single_node_es_host }}",
    "fracBackendHost": "{{ dashboards_frac_backend_host }}",
    "cassandraUserKeyspace": "{{ user_table_keyspace }}",
    "cassandraCourseKeyspace": "{{ course_keyspace }}",
    "cassandraHierarchyStoreKeyspace": "{{ hierarchy_store_keyspace }}",
    "cassandraUserTable": "{{ dashboards_cassandra_user_table }}",
    "cassandraUserRolesTable": "{{ dashboards_cassandra_user_roles_table }}",
    "cassandraOrgTable": "{{ dashboards_cassandra_org_table }}",
    "cassandraUserEnrolmentsTable": "{{ dashboards_cassandra_user_enrolments_table }}",
    "cassandraContentHierarchyTable": "{{ dashboards_cassandra_content_hierarchy_table }}",
    "cassandraRatingSummaryTable": "{{ dashboards_cassandra_rating_summary_table }}",
    "cassandraRatingsTable": "{{ dashboards_cassandra_ratings_table }}",
    "cassandraUserAssessmentTable": "{{ dashboards_cassandra_user_assessment_table }}",
    "cassandraOrgHierarchyTable": "{{ dashboards_cassandra_org_hierarchy_table }}",
    "cassandraCourseBatchTable": "{{ dashboards_cassandra_course_batch_table }}",
    "cassandraLearnerStatsTable": "{{ dashboards_cassandra_learner_stats_table }}",
    "cassandraAcbpTable": "{{ dashboards_cassandra_acbp_table }}",
    "cassandraKarmaPointsLookupTable": "{{ dashboards_cassandra_karma_points_lookup_table }}",
    "cassandraKarmaPointsTable": "{{ dashboards_cassandra_karma_points_table }}",
    "cassandraHallOfFameTable": "{{ dashboards_cassandra_mdo_karma_points_table }}",
    "appPostgresHost": "{{ app_postgres_host }}",
    "appPostgresSchema": "sunbird",
    "appPostgresUsername": "sunbird",
    "appPostgresCredential": "sunbird",
    "appOrgHierarchyTable": "org_hierarchy_v4",
    "dwPostgresHost": "{{ dw_postgres_host }}",
    "dwPostgresSchema": "warehouse",
    "dwPostgresUsername": "postgres",
    "dwPostgresCredential": "{{ dw_postgres_credential }}",
    "dwUserTable": "user_detail",
    "dwCourseTable": "content",
    "dwEnrollmentsTable": "user_enrolments",
    "dwOrgTable": "org_hierarchy",
    "dwAssessmentTable": "assessment_detail",
    "dwBPEnrollmentsTable": "bp_enrolments",
    "key": "{{ dp_storage_key_config }}",
    "secret": "{{ dp_storage_secret_config }}",
    "store": "{{ report_storage_type }}",
    "container": "{{ s3_storage_container }}",
    "mdoIDs": "'$reportMDOIDs'",
    "cutoffTime": "60.0",
    "userReportPath": "{{ user_report_path }}",
    "userEnrolmentReportPath": "{{ user_enrolment_report_path }}",
    "courseReportPath": "{{ course_report_path }}",
    "cbaReportPath": "{{ cba_report_path }}",
    "standaloneAssessmentReportPath": "{{ standalone_assessment_report_path }}",
    "taggedUsersPath": "{{ tagged_users_path }}",
    "blendedReportPath": "{{ blended_report_path }}",
    "orgHierarchyReportPath": "{{ org_hierarchy_report_path }}",
    "commsConsoleReportPath": "{{ comms_console_report_path }}",
    "acbpReportPath": "{{ acbp_report_path }}",
    "acbpMdoEnrolmentReportPath": "{{ acbp_mdo_enrolment_report_path }}",
    "acbpMdoSummaryReportPath": "{{ acbp_mdo_summary_report_path }}",
    "commsConsolePrarambhEmailSuffix": "{{ comms_console_prarambh_email_suffix }}",
    "commsConsoleNumDaysToConsider": "{{ comms_console_num_days_to_consider }}",
    "commsConsoleNumTopLearnersToConsider": "{{ comms_console_num_top_learners_to_consider }}",
    "commsConsolePrarambhTags": "{{ comms_console_prarambh_tags }}",
    "commsConsolePrarambhCbpIds": "{{ comms_console_prarambh_cbp_ids }}",
    "commsConsolePrarambhNCount": "{{ comms_console_prarambh_n_count }}",
    "sideOutput": {
      "brokerList": "'$brokerList'",
      "compression": "{{ dashboards_broker_compression }}",
      "topics": {
        "roleUserCount": "{{ dashboards_role_count_topic }}",
        "orgRoleUserCount": "{{ dashboards_org_role_count_topic }}",
        "allCourses": "{{ dashboards_courses_topic }}",
        "userCourseProgramProgress": "{{ dashboards_user_course_program_progress_topic }}",
        "fracCompetency": "{{ dashboards_frac_competency_topic }}",
        "courseCompetency": "{{ dashboards_course_competency_topic }}",
        "expectedCompetency": "{{ dashboards_expected_competency_topic }}",
        "declaredCompetency": "{{ dashboards_declared_competency_topic }}",
        "competencyGap": "{{ dashboards_competency_gap_topic }}",
        "userOrg": "{{ dashboards_user_org_topic }}",
        "org": "{{ dashboards_org_topic }}",
        "userAssessment": "{{ dashboards_user_assessment_topic }}",
        "assessment": "{{ dashboards_assessment_topic }}",
        "acbpEnrolment": "{{ dashboards_acbp_enrolment_topic }}"
      }
    }
  },
  "output": [],
  "parallelization": 16,
  "appName": "ACBP Report Job",
  "deviceMapping": false
}
```

### How are the jobs being run?
Data products jobs are scheduled in crontab, (Walk-through with WFS as an example)

[run job script](../ansible/roles/data-products-deploy/templates/run-job.j2)

### Dashboard Jobs

https://github.com/sunbird-cb/sunbird-core-dataproducts/tree/cbrelease-4.8.11/batch-models/src/main/scala/org/ekstep/analytics/dashboard

- DashboardUtil
- DataUtil
- TestUtil

## DevOps
Analytics Core
- build - https://github.com/sunbird-cb/sunbird-analytics-core/blob/release-4.8.0-nic/Jenkinsfile
- deploy - [Jenkinsfile](../pipelines/deploy/data-products/Jenkinsfile)

Data Products
- build - https://github.com/sunbird-cb/sunbird-core-dataproducts/blob/cbrelease-4.8.11/Jenkinsfile
- deploy - [Jenkinsfile](../pipelines/deploy/data-products/Jenkinsfile)

## Debugging

```shell

# ssh to spark server
sudo su analytics

# check crontab
crontab -l

# running spark shell
cd /mount/data/analytics/spark-2.4.4-bin-hadoop2.7/
bin/spark-shell --master local[*] --packages redis.clients:jedis:4.2.3,com.datastax.spark:spark-cassandra-connector_2.11:2.5.0,org.mongodb.spark:mongo-spark-connector_2.11:2.4.2,org.apache.spark:spark-avro_2.11:2.4.0 --conf spark.cassandra.connection.host=192.168.3.246 --conf spark.sql.caseSensitive=true --jars /mount/data/analytics/models-2.0/analytics-framework-2.0.jar,/mount/data/analytics/models-2.0/scruid_2.11-2.4.0.jar
# :load /home/analytics/spark-scripts/DashboardUtil.scala
# :load /home/analytics/spark-scripts/DataUtil.scala
# :load /home/analytics/spark-scripts/TestUtil.scala
# :load /home/analytics/spark-scripts/DataExhaustModel.scala
# TestUtil.main(DataExhaustModel)

# convert avro files to json
cd /mount/data/analytics/cache
wget https://repo1.maven.org/maven2/org/apache/avro/avro-tools/1.9.1/avro-tools-1.9.1.jar  # may already be present
java -jar avro-tools-1.9.1.jar tojson some.avro > some.json
# we have multiple partitions of these files so we extract in a loop
ls orgHierarchy/*.avro | while read LINE; do java -jar avro-tools-1.9.1.jar tojson "$LINE" >> orgHierarchy.json; done

# check logs
cd /mount/data/analytics/scripts/logs
tail -f joblog.log

# detailed spark logs
cd /mount/data/analytics/logs/data-products
ls -ltrh

```

