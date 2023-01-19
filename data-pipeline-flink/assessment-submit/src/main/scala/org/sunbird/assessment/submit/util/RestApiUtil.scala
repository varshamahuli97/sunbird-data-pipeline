package org.sunbird.assessment.submit.util

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.PropertyAccessor
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.ScalaObjectMapper
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.DefaultHttpClient
import org.slf4j.LoggerFactory

import java.nio.charset.StandardCharsets


class RestApiUtil extends Serializable {

  private[this] val logger = LoggerFactory.getLogger(classOf[RestApiUtil])
  logger.info("RestApi Call start")

  def postRequestWithContentType(uri: String, params: java.util.Map[String, Any]): String = {
    val post = new HttpPost(uri)
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.setVisibility(PropertyAccessor.ALL, Visibility.ANY)
    val jsonString = mapper.writeValueAsString(params)
    logger.info("json String " + jsonString)
    post.setHeader("Content-type", "application/json")
    post.setEntity(new StringEntity(jsonString))
    val client = new DefaultHttpClient()
    val response = client.execute(post)
    val statusCode = response.getStatusLine.getStatusCode
    if (statusCode.equals(200)) {
      logger.info("Rest Call successfully working")
    }
    val text = new String(response.getEntity.getContent.readAllBytes(), StandardCharsets.UTF_8);
    text
  }

  def getRequestForUserRead(uri: String): String = {
    val get = new HttpGet(uri)
    get.setHeader("Content-type", "application/json")
    get.setHeader("Authorization", "bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJSSXZtaHRpTkxRT1lKT3dYR2xnRElReFp4bHdyZmVTZCJ9.onjwk3QTql0oZYvM-xOPuCDcBJKGTVa65J64j2hy8H0")
    get.setHeader("X-Authenticated-User-Token", "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJVMkpUdlpERFY4eG83ZmtfNHd1Yy1kNVJmNjRPTG1oemlRRUhjR25Vc2hNIn0.eyJqdGkiOiJkYWMxMWNiYy0yNWQ1LTQ5MzktYTM0OS01NWRkNjE5MWE0MGEiLCJleHAiOjE2NzI3MzgwNDcsIm5iZiI6MCwiaWF0IjoxNjcyNjUxNjQ3LCJpc3MiOiJodHRwczovL3BvcnRhbC5pZ290LWRldi5pbi9hdXRoL3JlYWxtcy9zdW5iaXJkIiwic3ViIjoiZjo5MjNiZGMxOC01MjBkLTQ4ZDQtYTg0ZS0zY2RlMWU2NTVlYmQ6ZGE5YjU0Y2ItNzYzNS00MDE2LWE1NDMtYTFmM2ZiMmZhYjhjIiwidHlwIjoiQmVhcmVyIiwiYXpwIjoiYWRtaW4tY2xpIiwiYXV0aF90aW1lIjowLCJzZXNzaW9uX3N0YXRlIjoiM2MwODEyMWUtNjIzMy00M2Y1LWFiNTgtMjBmMjJlZjg1OWU4IiwiYWNyIjoiMSIsInJlYWxtX2FjY2VzcyI6eyJyb2xlcyI6WyJvZmZsaW5lX2FjY2VzcyIsInVtYV9hdXRob3JpemF0aW9uIl19LCJzY29wZSI6IiIsIm5hbWUiOiJKdWhpIENCUCBBZG1pbiIsInByZWZlcnJlZF91c2VybmFtZSI6Imp1aGljYnBhZG1pbl92Y3BoIiwiZ2l2ZW5fbmFtZSI6Ikp1aGkiLCJmYW1pbHlfbmFtZSI6IkNCUCBBZG1pbiIsImVtYWlsIjoianUqKioqKioqQHlvcG1haWwuY29tIn0.a0l36bWCKbDvcDjhxg6PzT48lt8QN8vMLyc_wiD995_viUjblH_LL9rH9eelTUcLdhEnxI5Pk8TOL0K1PAyE8vNkz6ZFY99Y2xs1VObC4A0lp-JuyMVIm_ANRkwWaszPQl4D77O-OyWanV4n2fQOETVx0ENYQ2QNQBKqTMnA1rwzTFVOiahAO2VWY3AQIBsOMnpGsQvW2hqkXoSarmKxAWBaM4F2tRa9vyiHvwB5RlJyARRoEIbWZ3q3tWObmtA1LR4dBkL7QHP7Z3TihRG2A2F59UhQOFwR0P9cw5S2gu7ZwMF8vzQUqee6di--WAbnFfCCJ96_ZwaQAdCeP-rWFg")
    val client = new DefaultHttpClient()
    val response = client.execute(get)
    val statusCode = response.getStatusLine.getStatusCode
    if (statusCode.equals(200)) {
      logger.info("Rest get Call successfully working")
    }
    val text = new String(response.getEntity.getContent.readAllBytes(), StandardCharsets.UTF_8);
    text
  }
}

