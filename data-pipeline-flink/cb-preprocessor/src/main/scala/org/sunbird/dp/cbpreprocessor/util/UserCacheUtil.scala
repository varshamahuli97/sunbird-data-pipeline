package org.sunbird.dp.cbpreprocessor.util

import org.sunbird.dp.cbpreprocessor.task.CBPreprocessorConfig
import org.sunbird.dp.core.cache.RedisConnect
import redis.clients.jedis.Jedis
import redis.clients.jedis.exceptions.{JedisException, JedisConnectionException}

class UserCacheUtil(config: CBPreprocessorConfig, redisConnect: RedisConnect, store: Int) extends Serializable {

  private val serialVersionUID = 6089562751616425355L
  private[this] var redisConnection: Jedis = redisConnect.getConnection
  redisConnection.select(store)

  @throws[JedisException]
  @throws[JedisConnectionException]
  def getUserOrg(userId: String): (String, String) = {
    val cacheData = redisConnection.hgetAll(config.userStoreKeyPrefix + userId)
    (cacheData.getOrDefault(config.rootOrgId, ""), cacheData.getOrDefault(config.orgnameKey, ""))
  }

  @throws[JedisException]
  @throws[JedisConnectionException]
  def getUserOrgWithRetry(userId: String): (String, String) = {
    var orgData = ("", "")
    try {
      orgData = getUserOrg(userId)
    } catch {
      case ex@(_: JedisException | _: JedisConnectionException) =>
        ex.printStackTrace()
        this.redisConnection.close()
        this.redisConnection = redisConnect.getConnection(this.store, backoffTimeInMillis = 10000)
        orgData = getUserOrg(userId)
    }
    orgData
  }

  def getRedisConnection: Jedis = redisConnection

  def close(): Unit = {
    redisConnection.close()
  }
}
