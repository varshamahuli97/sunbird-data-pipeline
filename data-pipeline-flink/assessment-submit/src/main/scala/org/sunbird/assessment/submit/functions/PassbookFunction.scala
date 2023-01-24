package org.sunbird.assessment.submit.functions


import com.google.gson.Gson
import org.apache.commons.collections.MapUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.assessment.submit.domain.Event
import org.sunbird.assessment.submit.task.AssessmentConfig
import org.sunbird.assessment.submit.util.RestApiUtil
import org.sunbird.dp.contentupdater.core.util.RestUtil
import org.sunbird.dp.core.cache.{DataCache, RedisConnect}
import org.sunbird.dp.core.job.{BaseProcessFunction, Metrics}
import org.sunbird.dp.core.util.CassandraUtil
import java.util

class PassbookFunction(config: AssessmentConfig,
                       @transient var cassandraUtil: CassandraUtil = null
                      )(implicit val mapTypeInfo: TypeInformation[Event])
  extends BaseProcessFunction[Event, Event](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[PassbookFunction])
  private var dataCache: DataCache = _
  private var contentCache: DataCache = _
  private var restUtil: RestUtil = _
  private var restApiUtil: RestApiUtil = _


  override def metricsList() = List(config.updateCount, config.failedEventCount)

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    dataCache = new DataCache(config, new RedisConnect(config.metaRedisHost, config.metaRedisPort, config), config.relationCacheNode, List())
    dataCache.init()
    contentCache = new DataCache(config, new RedisConnect(config.metaRedisHost, config.metaRedisPort, config), config.contentCacheNode, List())
    contentCache.init()
    cassandraUtil = new CassandraUtil(config.dbHost, config.dbPort)
    restUtil = new RestUtil()
    restApiUtil = new RestApiUtil()
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
      logger.info("Entering PassbookFunction " + event.toString);
      if (event.actor.get("id").equalsIgnoreCase(config.CERTIFICATE_GENERATOR)) {
        val related = event.edata.get(config.RELATED).asInstanceOf[util.HashMap[String, Any]]
        val courseId = related.get(config.COURSE_ID)
        val competencyV3Details = enrichCompetencyV3(courseId);
      }
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        logger.info(s"Passbook update Failed with exception ${ex.getMessage}:")
        event.markFailed(ex.getMessage)
        context.output(config.failedEventsOutputTag, event)
        metrics.incCounter(config.failedEventCount)
    }
  }

  def enrichCompetencyV3(courseId: Any): util.List[Any] = {
    try {
      var competencyV3 = new util.ArrayList[Any]()
      val filters = new util.HashMap[String, Any]()
      filters.put(config.IDENTIFIER, courseId)
      filters.put(config.STATUS, new util.ArrayList[String]() {
        add("Live")
      })
      filters.put(config.PRIMARY_CATEGORY, new util.ArrayList[String]() {
        add("Course")
      })
      val sortBy=new util.HashMap[String,Any]()
      sortBy.put(config.lastUpdatedOn,config.DESC)
      val request = new util.HashMap[String, Any]()
      request.put(config.FILTERS, filters)
      request.put(config.SORT_BY, sortBy)
      request.put(config.FIELDS, "competencies_v3")
      val requestBody = new util.HashMap[String, Any]()
      requestBody.put(config.REQUEST, request)
      val url: String = config.KM_BASE_HOST + config.content_search
      val obj = restApiUtil.post(url, requestBody)
      val gson = new Gson()
      val response = gson.fromJson(obj, classOf[util.Map[String, Any]])
      if (MapUtils.isNotEmpty(response)) {
        val result = response.get(config.RESULT).asInstanceOf[util.Map[String, Any]]
        if (result.get(config.CONTENT) != null) {
          val contentList: util.List[util.Map[String, Any]] = result.get(config.CONTENT).asInstanceOf[util.List[util.Map[String, Any]]]
          contentList.forEach(content => {
            competencyV3.add(content.get("competencies_v3"))
          })
        }
      }
      logger.info("competency v3: "+competencyV3)
      competencyV3
    }
    catch {
      case e: Exception => e.printStackTrace()
        logger.info(String.format("Failed during fetching mail %s", e.getMessage()))
        null
    }
  }
}
