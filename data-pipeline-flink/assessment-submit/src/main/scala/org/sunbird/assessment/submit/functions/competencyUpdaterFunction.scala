package org.sunbird.assessment.submit.functions


import com.google.gson.Gson
import org.apache.commons.collections.MapUtils
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.fasterxml.jackson.databind.ObjectMapper
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
import java.util.Map
import scala.collection.convert.ImplicitConversions.`map AsJavaMap`

class competencyUpdaterFunction(config: AssessmentConfig,
                                @transient var cassandraUtil: CassandraUtil = null
                      )(implicit val mapTypeInfo: TypeInformation[Event])
  extends BaseProcessFunction[Event, Event](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[competencyUpdaterFunction])
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
      logger.info("Entering PassbookFunction " + event.toString)
      if (event.actor.get("id").equalsIgnoreCase(config.CERTIFICATE_GENERATOR)) {
        val userId = event.edata.get(config.USER_ID)
        logger.info ("user id of this user is :"+userId)
        val related = event.edata.get(config.RELATED).asInstanceOf[util.HashMap[String, Any]]
        val courseId = related.get(config.COURSE_ID)
        logger.info("new one new")
        val courseCompetenciesList = enrichCompetencyV3(courseId).asInstanceOf[util.List[util.Map[String, Any]]]
        val courseCompetencies : util.List[util.Map[String, Any]] = courseCompetenciesList.get(0).asInstanceOf[util.List[util.Map[String, Any]]]
        logger.info("Course competencies"+ courseCompetenciesList)
        logger.info("Course competencies new"+ courseCompetencies)
        val profileDetails = getUserProfileDetails(userId).asInstanceOf[util.Map[String, Any]]
        logger.info("before competency")
        var k: Int = 0
        val courseCompetenciesMapJava = new java.util.HashMap[String, Object]()
        courseCompetencies.forEach(content => {
          //          courseCompetenciesIdList.add()
          logger.info("inside loop courseCompetencies.forEach" + content.toString)
          logger.info("inside content" + content.get("id"))
          logger.info("inside content string new" + content.get("id").toString)

          courseCompetenciesMapJava.put(content.get("id").toString, content)
          logger.info("inside course competency loop loop")

        })
        val courseCompetenciesMap: Map[String, Any] = courseCompetenciesMapJava.asInstanceOf[util.Map[String, Any]]

        if (profileDetails.containsKey(config.competencies)) {
          k = 1
          var userCompetencies: util.List[Map[String, Any]] = profileDetails.getOrDefault(config.competencies, null).asInstanceOf[util.List[Map[String, Any]]]
          logger.info("after competency" + userCompetencies)
          val userCompetenciesMapJava = new java.util.HashMap[String, Object]()
          logger.info("java new")
          //        m.put("Foo", java.lang.Boolean.TRUE)
          //        m.put("Bar", java.lang.Integer.valueOf(1))
          //        val m2: Map[String, Any] = m.asInstanceOf[util.Map[String, Any]]
          //        var courseCompetenciesMap : util.Map[String, Any] = null
          //        var userCompetenciesMap : util.Map[String, Any] = null
          //        var userCompetenciesIdList : List[String] = null
          //        var courseCompetenciesIdList : util.List[String] = null
          logger.info("before competency mapper")

          userCompetencies.forEach(content => {
            logger.info("inside loop userCompetencies.forEach" + content.toString)
            userCompetenciesMapJava.put(content.get(config.id).asInstanceOf[String], content)
            logger.info("inside user competency loop")

          })
          val userCompetenciesMap: Map[String, Any] = userCompetenciesMapJava.asInstanceOf[util.Map[String, Any]]
          logger.info("course map" + courseCompetenciesMap + "  usermap" + userCompetenciesMap)

          courseCompetencies.forEach(courseContent => {
            logger.info("inside course competency loop int")
            var courseCID: String = courseContent.get(config.id).asInstanceOf[String]
            var courseLevelValue: String = courseContent.get(config.selectedLevelLevel).asInstanceOf[String]
            var courseLevelName: String = courseContent.get(config.selectedLevelName).asInstanceOf[String]
            val competencyLevel = courseLevelValue.split(" ")(1)
            val competencyIntLevel = competencyLevel.toInt
            logger.info("int error resolved new" + "   " + competencyLevel + "   " + competencyIntLevel)
            if (userCompetenciesMap.containsKey(courseCID)) {
              logger.info("inside user course loop")
              var userContent: Map[String, Any] = userCompetenciesMap.get(courseCID).asInstanceOf[Map[String, Any]]
              var userLevelValue: String = userContent.get(config.competencyCBPCompletionLevel).asInstanceOf[String]
              logger.info("user level" + userLevelValue)
              var userLevel: Int = 0
              if (null != userLevelValue) {
                logger.info("inside level condition")
                var userLevel: Int = userLevelValue.toInt
                logger.info("competencylevel " + competencyIntLevel + "    " + "userlevel" + userLevel)

                if (null != competencyIntLevel) {
                  if (competencyIntLevel > userLevel) {
                    logger.info("inside competency user level check")
                    userContent.put(config.competencyCBPCompletionLevel, competencyLevel)
                    userContent.put(config.competencyCBPCompletionLevelValue, courseLevelValue)
                    userContent.put(config.competencyCBPCompletionLevelName, courseLevelName)
                  }
                }
              } else {
                logger.info("inside else part")
                userContent.put(config.competencyCBPCompletionLevel, competencyLevel)
                userContent.put(config.competencyCBPCompletionLevelValue, courseLevelValue)
                userContent.put(config.competencyCBPCompletionLevelName, courseLevelName)
              }

            } else {
              var newCompetencyMap: Map[String, Any] = new util.HashMap()
              newCompetencyMap.put(config.id, courseCID)
              newCompetencyMap.put(config.name, courseContent.get(config.name).asInstanceOf[String])
              newCompetencyMap.put(config.description, courseContent.get(config.description).asInstanceOf[String])
              newCompetencyMap.put(config.types, config.competency)
              newCompetencyMap.put(config.source, courseContent.get(config.source).asInstanceOf[String])
              newCompetencyMap.put(config.competencyCBPCompletionLevel, competencyLevel)
              newCompetencyMap.put(config.competencyCBPCompletionLevelValue, courseLevelValue)
              newCompetencyMap.put(config.competencyCBPCompletionLevelName, courseLevelName)
              newCompetencyMap.put(config.osid, java.util.UUID.randomUUID.toString)
              userCompetencies.add(newCompetencyMap)
              logger.info("New Competency Map With Added Competency old ${userCompetencies} :")
              profileDetails.put(config.competencies, userCompetencies.toString)
              logger.info("Newly Updated Profile Details ${profileDetails} :")
            }
          })
        } else {
          courseCompetencies.forEach(courseContent => {
            logger.info("inside course competency loop int else part")
            var courseCID: String = courseContent.get(config.id).asInstanceOf[String]
            var courseLevelValue: String = courseContent.get(config.selectedLevelLevel).asInstanceOf[String]
            var courseLevelName: String = courseContent.get(config.selectedLevelName).asInstanceOf[String]
            val competencyLevel = courseLevelValue.split(" ")(1)

            var newCompetencyMap: Map[String, Any] = new util.HashMap()
            newCompetencyMap.put(config.id, courseCID)
            newCompetencyMap.put(config.name, courseContent.get(config.name).asInstanceOf[String])
            newCompetencyMap.put(config.description, courseContent.get(config.description).asInstanceOf[String])
            newCompetencyMap.put(config.types, config.competency)
            newCompetencyMap.put(config.source, courseContent.get(config.source).asInstanceOf[String])
            newCompetencyMap.put(config.competencyCBPCompletionLevel, competencyLevel)
            newCompetencyMap.put(config.competencyCBPCompletionLevelValue, courseLevelValue)
            newCompetencyMap.put(config.competencyCBPCompletionLevelName, courseLevelName)
            newCompetencyMap.put(config.osid, java.util.UUID.randomUUID.toString)
            var userCompetenciesList: util.List[Map[String, Any]] = new util.ArrayList[Map[String, Any]]()
            userCompetenciesList.add(newCompetencyMap)
            logger.info(" else part New Competency Map With Added Competency old ${userCompetencies} :")
            profileDetails.put(config.competencies, userCompetenciesList)
            logger.info(" else part Newly Updated Profile Details ${profileDetails} :")
          })
        }
        logger.info("profile details" + profileDetails)
        updateProfileDetails(profileDetails, userId)
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
      request.put(config.FIELDS, config.competencies_v3)
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
            competencyV3.add(content.get(config.competencies_v3))
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

  def getUserProfileDetails(userId: String): util.Map[String, Any]  = {
    if (!userId.isEmpty) {
      val query = QueryBuilder.select(config.profileDetails)
        .from(config.dbSunbirdKeyspace, config.userTable)
        .where(QueryBuilder.eq(config.userId, userId)).allowFiltering().toString
      val record = cassandraUtil.findOne(query)
      val profileDetails = record.getString(config.profileDetails)
      logger.info(s"profileDetails${profileDetails} :")
      if (!profileDetails.isEmpty || profileDetails != null) {
        val mapper = new ObjectMapper
        val profileDetailsMapper = mapper.readValue(profileDetails, classOf[Map[String, Any]])
        logger.info(s"profileDetailsMapper: ${profileDetailsMapper} :")
        profileDetailsMapper
      } else {
        logger.info(s"competencies: ${"competencies"} :")
        return null
      }
    } else {
      return null
    }
  }

  def updateProfileDetails(profileDetails:  util.Map[String, Any], userId: String): Unit = {
    try {
      val mapper = new ObjectMapper
      logger.info("inside update method"+"   "+"userid "+userId)
      val query = QueryBuilder.update(config.dbSunbirdKeyspace, config.userTable)
        .`with`(QueryBuilder.set(config.profileDetails, mapper.writeValueAsString(profileDetails)))
        .where(QueryBuilder.eq(config.id, userId))
      logger.info(s"Query: ${query}")
      cassandraUtil.upsert(query.toString)
    } catch {
      case exception: Exception =>
        exception.printStackTrace()
        logger.info("Updating Profile Details Failed :"+exception.getMessage())
    }
  }
}
