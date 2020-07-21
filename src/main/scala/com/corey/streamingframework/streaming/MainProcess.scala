package com.corey.streamingframework.streaming

import java.lang
import java.text.MessageFormat

import com.corey.streamingframework.Utils.{CaseConf, StreamingUtils}
import com.corey.streamingframework.busi.BusinessProcess
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

object MainProcess {
  private val processLog: Log = LogFactory.getLog(MainProcess.getClass)

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("args prop file path is null!")
      System.exit(1)
    }

    var needAuth = false

    if (args.length > 1 && args(2).equalsIgnoreCase("true")) {
      needAuth = true
    }

    init(args(0), needAuth)
  }

  def init(confPath: String, needAuth: Boolean): Unit = {
    try {
      val caseConf = new CaseConf(confPath)
      val checkpointDir: String = caseConf.get("framework.streaming.checkpointDir")
      val appName: String = caseConf.get("framework.streaming.appName", "MyApp")
      val banchTime: Int = caseConf.get("framework.streaming.banchTime", "10").toInt

      val ssc: StreamingContext = StreamingContext.getOrCreate(checkpointDir, () => {
        createContext(appName, banchTime, checkpointDir, caseConf, needAuth)
      })

      ssc.start()
      ssc.awaitTermination()
    } catch {
      case e: Exception => {
        processLog.error(e.getMessage, e)
      }
    }
  }

  def createContext(appName: String, banchTime: Int, checkpointDir: String, caseConf: CaseConf, needAuth: Boolean): StreamingContext = {
    val sparkConf: SparkConf = new SparkConf().setAppName(appName).setIfMissing("spark.master", "local[*]")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerialzer")
    val arr = ArrayBuffer[Class[_]]()
    arr.append(classOf[CaseConf])

    val needKryoSerClassName: String = caseConf.get("framework.streaming.needKryoSerClassName", "").trim

    if (!"".equals(needKryoSerClassName)) {
      val classNames: Array[String] = needKryoSerClassName.split(",")
      for (elem <- classNames) {
        arr.append(Class.forName(elem))
      }
    }

    sparkConf.registerKryoClasses(arr.toArray)

    val busImpClassName: String = caseConf.get("freamwork.streaming.busImpClassName")
    val pro = Class.forName(busImpClassName).newInstance().asInstanceOf[BusinessProcess]
    val sparkConf1: SparkConf = pro.addConf(sparkConf, caseConf)
    val sc = new SparkContext(sparkConf1)
    val sc1: SparkContext = pro.initSC(sc, caseConf)

    val ssc = new StreamingContext(sc1, Seconds(banchTime))
    ssc.checkpoint(checkpointDir)

    try {
      process(ssc, caseConf, pro, needAuth)
    } catch {
      case e: Exception =>
        processLog.error(e.getMessage, e)
    }

    ssc
  }

  def process(ssc: StreamingContext, caseConf: CaseConf, pro: BusinessProcess, needAuth: Boolean) = {
    val brokers: String = caseConf.get("framework.input.brokerList")
    val securityPort: String = caseConf.get("framework.input.securityPort", "9093")
    val emptyProcess: String = caseConf.get("framework.rdd.empty.process", "false")

    var kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> caseConf.get("framework.input.group"),
      "auto.offset.reset" -> caseConf.get("framework.streaming.auto.offser.reset", "latest").toLowerCase,
      "enable.auto.commit" -> (false: lang.Boolean)
    )

    if (brokers.endsWith(securityPort)) {
      kafkaParams += ("security.protocol" -> "SASL_PLAINTEXT")
      kafkaParams += ("sasl.mechanism" -> "PLAIN")
      val username: String = caseConf.get("framework.input.kafka.username","")
      val password: String = caseConf.get("framework.input.kafka.password","")

      if("".equals(username) || "".equals(password)){
        System.out.println("输入的kafka用户名或者密码为空，请在配置文件中添加用户名和密码配置项！")
        processLog.warn("输入的kafka用户名或者密码为空，请在配置文件中添加用户名和密码配置项！")
        System.exit(1)
      }

      val jaasTemplate:String ="org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{0}\" password=\"{1}\";"
      kafkaParams +=("sasl.jaas.config" -> MessageFormat.format(jaasTemplate,username,password))
    }

    val topicSet: Array[String] = caseConf.get("framework.input.topics").split(",")
    val logStream:InputDStream[ConsumerRecord[String,String]] = StreamingUtils.createDirectStream(ssc,kafkaParams,topicSet,caseConf)

    logStream.foreachRDD{ rdd =>{
      if(!rdd.isEmpty()){
        val offsetsArray: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        val valueRDD: RDD[String] = rdd.map(x => x.value())
        pro.busProcessImp(valueRDD,caseConf)
        StreamingUtils.saveOffset(offsetsArray,caseConf)
      } else {
        println("input: " + 0)

        if("true".equals(emptyProcess)){
          val valueRDD: RDD[String] = rdd.map(x => x.value())
          pro.busProcessImp(valueRDD,caseConf)
        }
      }
    }

    }

  }

}
