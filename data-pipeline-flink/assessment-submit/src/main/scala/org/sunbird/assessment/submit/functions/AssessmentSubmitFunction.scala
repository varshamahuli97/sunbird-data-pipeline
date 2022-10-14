package org.sunbird.assessment.submit.functions
import com.datastax.driver.core.Row
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.reflect.TypeToken
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.joda.time.{DateTime, DateTimeZone}
import org.slf4j.LoggerFactory
import org.sunbird.assessment.submit.domain.Event
import org.sunbird.assessment.submit.task.AssessmentConfig
import org.sunbird.dp.contentupdater.core.util.RestUtil
import org.sunbird.dp.core.cache.{DataCache, RedisConnect}
import org.sunbird.dp.core.job.{BaseProcessFunction, Metrics}
import org.sunbird.dp.core.util.CassandraUtil
import com.fasterxml.jackson.databind.ObjectMapper

import scala.collection.JavaConverters._
import java.util
import java.util.{HashMap, Map}

class AssessmentSubmitFunction(config: AssessmentConfig,
                                   @transient var cassandraUtil: CassandraUtil = null
                                  )(implicit val mapTypeInfo: TypeInformation[Event])
  extends BaseProcessFunction[Event, Event](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[AssessmentSubmitFunction])
  private var dataCache: DataCache = _
  private var contentCache: DataCache = _
  private var restUtil: RestUtil = _


  override def metricsList() = List(config.updateCount, config.failedEventCount)


  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    dataCache = new DataCache(config, new RedisConnect(config.metaRedisHost, config.metaRedisPort, config), config.relationCacheNode, List())
    dataCache.init()
    contentCache = new DataCache(config, new RedisConnect(config.metaRedisHost, config.metaRedisPort, config), config.contentCacheNode, List())
    contentCache.init()
    cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort)
    restUtil = new RestUtil()
  }

  override def close(): Unit = {
    super.close()
  }

  /**
   * Method to write the assess event to cassandra table
   *
   * @param event   - Assess Batch Events
   * @param context - Process Context
   */
  override def processElement(event: Event,
                              context: ProcessFunction[Event, Event]#Context,
                              metrics: Metrics): Unit = {
    try {
      var totalScore: Double = 0.0
      var noOfAttempts: Int = 0
      if(event.primaryCategory.equalsIgnoreCase("Practice Question Set"))
        {
          totalScore = 50.0
        }
        else
        {
          totalScore = fetchScoreFromDatabase(event.userId, event.assessmentId)(metrics)
        }
        noOfAttempts = getNoOfAttemptsMadeForThisAssessment(event)
        logger.info(s"noOfAttempts ${noOfAttempts}")
      // Validating the contentId
        if((event.primaryCategory.equalsIgnoreCase("Practice Question Set") && noOfAttempts==0) || (event.primaryCategory.equalsIgnoreCase("Course Assessment") && totalScore>=0.0)) {
          logger.info("Saving to the Assessment Aggregator Table")
          saveAssessment(noOfAttempts, event, totalScore)
          metrics.incCounter(config.updateCount)
          context.output(config.updateSuccessEventsOutputTag, event)
        }
        else
        {
          logger.info("No Valid scenarios for this flink job")
        }
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        logger.info(s"Assessment Submit Failed with exception ${ex.getMessage}:")
        event.markFailed(ex.getMessage)
        context.output(config.failedEventsOutputTag, event)
        metrics.incCounter(config.failedEventCount)
    }
  }

  def getNoOfAttemptsMadeForThisAssessment(event: Event): Int = {
    val query = QueryBuilder.select("user_id")
      .from(config.dbCoursesKeyspace, config.table).
       where(QueryBuilder.eq("batch_id", event.batchId))
      .and(QueryBuilder.eq("user_id", event.userId))
      .and(QueryBuilder.eq("content_id", event.contentId))
      .and(QueryBuilder.eq("course_id", event.courseId)).allowFiltering().toString
    val records = cassandraUtil.find(query)
    if (records.isEmpty) 1 else records.size() + 1
  }

  def saveAssessment(attemptId: Int, event: Event, totalScore: Double): Unit = {
    try {
      val query = QueryBuilder.insertInto(config.dbCoursesKeyspace, config.table)
        .value("course_id", event.courseId).value("batch_id", event.batchId).value("user_id", event.userId)
        .value("content_id", event.contentId).value("attempt_id", attemptId.toString)
        .value("updated_on", new DateTime(DateTimeZone.UTC).getMillis).value("created_on", new DateTime(DateTimeZone.UTC).getMillis)
        .value("last_attempted_on", new DateTime(DateTimeZone.UTC).getMillis).value("total_score", totalScore)
        .value("total_max_score", 100.0).toString
      logger.info(s"Query: ${query}")
      cassandraUtil.upsert(query)
    } catch {
      case exception: Exception =>
        exception.printStackTrace()
        logger.info(s"Assessment Saving Failed with ${exception.getMessage} :")
    }
  }

  def fetchScoreFromDatabase(userId: String, assessmentId: String)(metrics: Metrics) = {
    val query = QueryBuilder.select("submitassessmentresponse")
      .from(config.dbSunbirdKeyspace, config.userAssessmentDataTable).
      where(QueryBuilder.eq("userid", userId))
      .and(QueryBuilder.eq("assessmentid", assessmentId)).limit(1).allowFiltering().toString
    val row = cassandraUtil.findOne(query)
    if (null != row) {
      val response = row.getString("submitassessmentresponse")
      logger.info(s"Submit Assessment Result 1${response} :")
      val mapper = new ObjectMapper
      val resp = mapper.readValue(response, classOf[Map[String, Any]])
      logger.info(s"Submit Assessment Result 2${resp} :")
      logger.info(s"Submit Assessment Result 3${resp.toString} :")
      val totalScore : Double = resp.getOrDefault("overallResult", 0.0).asInstanceOf[Double]
      logger.info(s"Total Score ${totalScore} :")
      totalScore
  }
    else {
      0.0
    }
  }

}
