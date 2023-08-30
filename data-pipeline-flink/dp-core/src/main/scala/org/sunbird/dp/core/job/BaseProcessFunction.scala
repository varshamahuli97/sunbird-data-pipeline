package org.sunbird.dp.core.job

import java.lang
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import org.apache.flink.api.scala.metrics.ScalaGauge
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{OutputTag, createTypeInformation}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

case class Metrics(metrics: ConcurrentHashMap[String, AtomicLong]) {
  def incCounter(metric: String): Unit = {
    metrics.get(metric).getAndIncrement()
  }

  def getAndReset(metric: String): Long = metrics.get(metric).getAndSet(0L)
  def get(metric: String): Long = metrics.get(metric).get()
  def reset(metric: String): Unit = metrics.get(metric).set(0L)
}

trait JobMetrics {
  def registerMetrics(metrics: List[String]): Metrics = {
    val metricMap = new ConcurrentHashMap[String, AtomicLong]()
    metrics.map { metric => metricMap.put(metric, new AtomicLong(0L)) }
    Metrics(metricMap)
  }
}

abstract class BaseProcessFunction[T, R](config: BaseJobConfig[T]) extends ProcessFunction[T, R] with BaseDeduplication with JobMetrics {

  private val metrics: Metrics = registerMetrics(List(config.uncaughtErrorEventCountMetric) ::: metricsList())
  private[this] val logger = LoggerFactory.getLogger(classOf[BaseProcessFunction[T, R]])

  override def open(parameters: Configuration): Unit = {
    metricsList().map { metric =>
      getRuntimeContext.getMetricGroup.addGroup(config.jobName)
        .gauge[Long, ScalaGauge[Long]](metric, ScalaGauge[Long]( () => metrics.getAndReset(metric) ))
    }
  }

  def processElement(event: T, context: ProcessFunction[T, R]#Context, metrics: Metrics): Unit
  def metricsList(): List[String]

  def handleUncaughtException(ex: Exception, event: T, context: ProcessFunction[T, R]#Context): Unit = {
    try {
      // log and recover
      ex.printStackTrace()
      logger.info("Uncaught exception: ", ex.getMessage)
      logger.info("Recovering: Sending event to uncaught error output")
      context.output(config.uncaughtErrorEventsOutputTag, event)
      metrics.incCounter(metric = config.uncaughtErrorEventCountMetric)
    } catch {
      case exRec: Exception => {
        // this should never happen!! only here till recovery code is tested to be perfect
        exRec.printStackTrace()
        logger.info("Exception encountered while recovering from exception: ", exRec.getMessage)
        // throw the exception in this case so that the we can debug for now
        throw exRec
      }
    }
  }

  override def processElement(event: T, context: ProcessFunction[T, R]#Context, out: Collector[R]): Unit = {
    try {
      processElement(event, context, metrics)
    } catch {
      case ex: Exception => handleUncaughtException(ex, event, context)
    }
  }
}

abstract class WindowBaseProcessFunction[I, O, K](config: BaseJobConfig[I]) extends ProcessWindowFunction[I, O, K, GlobalWindow] with BaseDeduplication with JobMetrics {

  private val metrics: Metrics = registerMetrics(List(config.uncaughtErrorEventCountMetric) ::: metricsList())
  private[this] val logger = LoggerFactory.getLogger(classOf[WindowBaseProcessFunction[I, O, K]])

  override def open(parameters: Configuration): Unit = {
    metricsList().map { metric =>
      getRuntimeContext.getMetricGroup.addGroup(config.jobName)
        .gauge[Long, ScalaGauge[Long]](metric, ScalaGauge[Long](() => metrics.getAndReset(metric)))
    }
  }

  def metricsList(): List[String]

  def handleUncaughtException(ex: Exception, elements: lang.Iterable[I], context: ProcessWindowFunction[I, O, K, GlobalWindow]#Context): Unit = {
    try {
      // log and recover
      ex.printStackTrace()
      logger.info("Uncaught exception: ", ex.getMessage)
      logger.info("Recovering: Sending event to uncaught error output")
      elements.forEach(event => {
        context.output(config.uncaughtErrorEventsOutputTag, event)
        metrics.incCounter(metric = config.uncaughtErrorEventCountMetric)
      })
    } catch {
      case exRec: Exception => {
        // this should never happen!! only here till recovery code is tested to be perfect
        exRec.printStackTrace()
        logger.info("Exception encountered while recovering from exception: ", exRec.getMessage)
        // throw the exception in this case so that the we can debug for now
        throw exRec
      }
    }
  }

  def process(key: K,
              context: ProcessWindowFunction[I, O, K, GlobalWindow]#Context,
              elements: lang.Iterable[I],
              metrics: Metrics): Unit

  override def process(key: K, context: ProcessWindowFunction[I, O, K, GlobalWindow]#Context, elements: lang.Iterable[I], out: Collector[O]): Unit = {
    try {
      process(key, context, elements, metrics)
    } catch {
      case ex: Exception => handleUncaughtException(ex, elements, context)
    }
  }
}