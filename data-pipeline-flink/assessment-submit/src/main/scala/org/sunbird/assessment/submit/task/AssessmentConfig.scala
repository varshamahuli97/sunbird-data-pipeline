package org.sunbird.assessment.submit.task

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.assessment.submit.domain.Event
import org.sunbird.dp.core.job.BaseJobConfig

class AssessmentConfig (override val config: Config) extends BaseJobConfig(config, "assessmentFeatureJob"){
  private val serialVersionUID = 2905979434303791379L

  implicit val mapTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])

  // Kafka Topics Configuration
  val inputTopic: String = config.getString("kafka.input.topic")

  // rating specific
  val ratingParallelism: Int = config.getInt("task.rating.parallelism")
  val kafkaIssueTopic: String = config.getString("kafka.output.topic")
  val kafkaSuccessTopic: String = config.getString("kafka.success.topic")

  val issueEventSink = "assessment-submit-issue-event-sink"
  val successIssueEventSink = "success-assessment-issue-event-sink"
  val issueOutputTagName = "assessment-sumit-issue-events"
  val failedEvent: OutputTag[Event] = OutputTag[Event]("failed-assessment-submit-events")
  val failedEventsOutputTag: OutputTag[Event] = OutputTag[Event]("assess-submit-failed-events")
  val updateSuccessEventsOutputTag: OutputTag[Event] = OutputTag[Event]("update-success-event-count")
  val dbUpdateCount = "db-update-count"
  val dbReadCount = "db-read-count"
  val batchSuccessCount = "batch-success-event-count"
  val failedEventCount = "failed-event-count"
  val ignoredEventsCount = "ignored-event-count"
  val skippedEventCount = "skipped-event-count"
  val cacheHitCount = "cache-hit-count"
  val cacheHitMissCount = "cache-hit-miss-count"
  val certIssueEventsCount = "cert-issue-events-count"
  val dbScoreAggUpdateCount = "db-score-update-count"
  val dbScoreAggReadCount = "db-score-read-count"
  val apiHitSuccessCount = "api-hit-success-count"
  val apiHitFailedCount = "api-hit-failed-count"
  val recomputeAggEventCount = "recompute-agg-event-count"
  val updateCount = "update-count"


  val relationCacheNode: Int = config.getInt("redis.database.relationCache.id")
  val contentCacheNode: Int = config.getInt("redis.database.contentCache.id")
  //Cassandra
  val table: String = config.getString("ext-cassandra.table")
  val dbHost: String = config.getString("ext-cassandra.host")
  val dbPort: Int = config.getInt("ext-cassandra.port")
  val dbCoursesKeyspace: String = config.getString("ext-cassandra.courses_keyspace")
  val dbSunbirdKeyspace: String = config.getString("ext-cassandra.keyspace")
  val userAssessmentDataTable: String = config.getString("ext-cassandra.user_assessment_data_table")

  // constants
  val courseId = "courseid"
  val userId = "userid"

  // Consumers
  val AssessmentConsumer = "assessment-submit-consumer"

  // Functions
  val assessmentFunction = "AssessmentSubmitFunction"
}
