package com.corey.handler

import com.corey.streamingframework.Utils.CaseConf
import com.corey.utils.JsonUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.slf4j.LoggerFactory


trait Handler[T] extends Serializable {

  val tBeanClass: Class[T]
  private val LOG = LoggerFactory.getLogger(getClass)

  /**
    * 程序步骤
    */

  def proceduce(msg: T, targeTopic: String, producer: KafkaProducer[String, String]): Unit

  /**
    * 解析数据源
    */
  def parseMsg(msg: (String, String)): Option[(String, T)] = {
    val key = msg._1
    val value = msg._2.trim

    if (value == "") {
      LOG.error(s"empty value,message[$key,$value]")
      None
    } else {
      val msgBean = JsonUtils.fromJson(value, tBeanClass)
      if (msgBean.isDefined) {
        Some(key, msgBean.get)
      } else {
        None
      }
    }
  }

  /**
    * 创建StreamingContext对象
    */

  def createContext(checkpointPath: String, interval: Duration): StreamingContext = {
    val conf = new SparkConf().setIfMissing("spark.master", "local[2]").setIfMissing("spark.app.name", "appInDev")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[CaseConf]))
    val ssc = new StreamingContext(conf, interval)
//    ssc.checkpoint(checkpointPath)
    ssc
  }

  /**
    * 发送消息
    */
  def sendMsg(key: String, topic: String, producer: KafkaProducer[String, String], value: String): Unit = {
    val producerRecord = new ProducerRecord[String, String](topic, key, JsonUtils.addTimeJson(value))
    LOG.debug(s"send a topic [$topic],record is [$key,$value]")

    println(value)
    producer.send(producerRecord)
  }
}

