package com.corey.streamingframework.Utils

import java.io.{BufferedReader, InputStreamReader}
import java.net.URI
import java.time.Duration
import java.util
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkNoNodeException
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.{PartitionInfo, TopicPartition}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{KafkaUtils, OffsetRange}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions

object StreamingUtils {
  private val log: Log = LogFactory.getLog(StreamingUtils.getClass)
  var zkClient: ZkClient = null

  def zkInit(properties: CaseConf) = {
    if (zkClient == null) {
      val zkHosts: String = properties.get("framework.streaming.zk.hosts")
      zkClient = ZkUtils.createZkClient(zkHosts, 180000, 180000)
    }
  }

  def zkInit(zkHosts: String) = {
    if (zkClient == null) {
      zkClient = ZkUtils.createZkClient(zkHosts, 180000, 180000)
    }
  }

  def createDirectStream(ssc: StreamingContext, kafkaParams: Map[String, Object], topics: Array[String], properties: CaseConf): InputDStream[ConsumerRecord[String, String]] = {

    zkInit(properties)
    val zkPath = s"${properties.get("framework.streaming.zk.path", "/kafkaOffset")}/${properties.get("framework.streaming.appName")}"
    val upgraded: Boolean = properties.get("framework.streaming.upgraded", "true").toBoolean
    val earlistTPOffset = new util.HashMap[TopicPartition, Long]()
    val latestTPOffset = new util.HashMap[TopicPartition, Long]()
    val currentTPOffset = new util.HashMap[TopicPartition, Long]()


    val jkafkaParams: util.Map[String, Object] = JavaConversions.mapAsJavaMap(kafkaParams)
    val consumer = new KafkaConsumer[String, String](jkafkaParams)
    val reset: String = kafkaParams.getOrElse("auto.offset.reset", "latest").asInstanceOf[String]

    println("auto.offset.reset:" + reset)
    log.info("auto.offset.reset:" + reset)
    var isNewApp: Boolean = true

    for (topic <- topics) {
      val topicPath = s"${zkPath}/${topic}"
      val children: Int = zkClient.countChildren(topicPath)
      if (children > 0) {
        isNewApp = false
      }

      val tps = new util.ArrayList[TopicPartition]()
      val patitionInfo: util.List[PartitionInfo] = consumer.partitionsFor(topic)
      try {
        JavaConversions.asScalaBuffer(patitionInfo).foreach(p => {
          val topicPartition = new TopicPartition(p.topic(), p.partition())
          tps.add(topicPartition)
        })
      } catch {
        case e: Exception => {
          if (e.getStackTrace.size > 0 && e.getStackTrace.apply(0).toString.contains("convert.Wrappers$JListWrapper")) {
            log.error(e.getMessage + " 请检查topic是否存在")
          } else {
            log.error(e.getMessage, e)
          }
        }
      }

      consumer.assign(tps)
      consumer.poll(Duration.ofMillis(1000))
      consumer.seekToBeginning(tps)

      JavaConversions.asScalaBuffer(tps).foreach(tp => {
        val earlistOffset: Long = consumer.position(tp)
        earlistTPOffset.put(tp, earlistOffset)
        currentTPOffset.put(tp, earlistOffset)
      })

      consumer.seekToEnd(tps)
      JavaConversions.asScalaBuffer(tps).foreach(tp => {
        val latestOffset: Long = consumer.position(tp)
        latestTPOffset.put(tp, latestOffset)
      })

      for (i <- 0 until children) {
        val partitionOffset = zkClient.readData[String](s"${topicPath}/$i")
        val tp = new TopicPartition(topic, i)
        val eOffset: Long = earlistTPOffset.get(tp)
        val lOffset: Long = latestTPOffset.get(tp)
        val pOffset: Long = partitionOffset.toLong

        if (pOffset < eOffset) {
          currentTPOffset.put(tp, eOffset)
          println("Offset头越界  topic[" + topic + "] patition [" + i + "] fromOffsets [" + partitionOffset + "] earlistOffset [" + eOffset + "]")
          log.warn("Offset头越界  topic[" + topic + "] patition [" + i + "] fromOffsets [" + partitionOffset + "] earlistOffset [" + eOffset + "]")
        } else if (pOffset > lOffset) {
          if ("latest".equals(reset)) {
            currentTPOffset.put(tp, lOffset)
          } else {
            currentTPOffset.put(tp, eOffset)
          }
          println("Offset头越界  topic[" + topic + "] patition [" + i + "] fromOffsets [" + partitionOffset + "] latestOffset [" + lOffset + "]")
          log.warn("Offset头越界  topic[" + topic + "] patition [" + i + "] fromOffsets [" + partitionOffset + "] latestOffset [" + lOffset + "]")
        } else {
          currentTPOffset.put(tp, pOffset)
        }
      }
    }

    System.out.println("earlistTPOffset: " + earlistTPOffset.toString)
    System.out.println("latestTPOffset: " + latestTPOffset.toString)
    System.out.println("currentTPOffset: " + currentTPOffset.toString)

    log.info("earlistTPOffset: " + earlistTPOffset.toString)
    log.info("latestTPOffset: " + latestTPOffset.toString)
    log.info("currentTPOffset: " + currentTPOffset.toString)

    var fromOffsets: scala.collection.mutable.Map[TopicPartition, Long] = null
    if (isNewApp || !upgraded) {
      if ("latest".equals(reset)) {
        fromOffsets = JavaConversions.mapAsScalaMap(latestTPOffset)
      } else {
        fromOffsets = JavaConversions.mapAsScalaMap(earlistTPOffset)
      }
      System.out.println("isNewApp: " + isNewApp + " | upgraded: " + upgraded)
      log.info("isNewApp: " + isNewApp + " | upgraded: " + upgraded)
    } else {
      fromOffsets = JavaConversions.mapAsScalaMap(currentTPOffset)
    }
    System.out.println("fromOffsets: " + fromOffsets.toString())
    log.info("fromOffsets: " + fromOffsets.toString())

    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams, fromOffsets))
    stream
  }

  def createDirectStream(ssc: StreamingContext, kafkaParams: Map[String, Object], topics: Array[String], zkHosts: String, upgraded: Boolean, offSetRootPath: String): InputDStream[ConsumerRecord[String, String]] = {

    zkInit(zkHosts)
    val appName: String = ssc.sparkContext.getConf.get("spark.app.name")
    if(appName.isEmpty){
      log.error("应用名不能为空！",new Exception)
      System.exit(1)
    }
    val zkPath = s"${offSetRootPath}/$appName"

    val earlistTPOffset = new util.HashMap[TopicPartition, Long]()
    val latestTPOffset = new util.HashMap[TopicPartition, Long]()
    val currentTPOffset = new util.HashMap[TopicPartition, Long]()


    val jkafkaParams: util.Map[String, Object] = JavaConversions.mapAsJavaMap(kafkaParams)
    val consumer = new KafkaConsumer[String, String](jkafkaParams)
    val reset: String = kafkaParams.getOrElse("auto.offset.reset", "latest").asInstanceOf[String]

    println("auto.offset.reset:" + reset)
    log.info("auto.offset.reset:" + reset)
    var isNewApp: Boolean = true

    for (topic <- topics) {
      val topicPath = s"${zkPath}/${topic}"
      val children: Int = zkClient.countChildren(topicPath)
      if (children > 0) {
        isNewApp = false
      }

      val tps = new util.ArrayList[TopicPartition]()
      val patitionInfo: util.List[PartitionInfo] = consumer.partitionsFor(topic)
      try {
        JavaConversions.asScalaBuffer(patitionInfo).foreach(p => {
          val topicPartition = new TopicPartition(p.topic(), p.partition())
          tps.add(topicPartition)
        })
      } catch {
        case e: Exception => {
          if (e.getStackTrace.size > 0 && e.getStackTrace.apply(0).toString.contains("convert.Wrappers$JListWrapper")) {
            log.error(e.getMessage + " 请检查topic是否存在")
          } else {
            log.error(e.getMessage, e)
          }
        }
      }

      consumer.assign(tps)
      consumer.poll(Duration.ofMillis(1000))
      consumer.seekToBeginning(tps)

      JavaConversions.asScalaBuffer(tps).foreach(tp => {
        val earlistOffset: Long = consumer.position(tp)
        earlistTPOffset.put(tp, earlistOffset)
        currentTPOffset.put(tp, earlistOffset)
      })

      consumer.seekToEnd(tps)
      JavaConversions.asScalaBuffer(tps).foreach(tp => {
        val latestOffset: Long = consumer.position(tp)
        latestTPOffset.put(tp, latestOffset)
      })

      for (i <- 0 until children) {
        val partitionOffset = zkClient.readData[String](s"${topicPath}/$i")
        val tp = new TopicPartition(topic, i)
        val eOffset: Long = earlistTPOffset.get(tp)
        val lOffset: Long = latestTPOffset.get(tp)
        val pOffset: Long = partitionOffset.toLong

        if (pOffset < eOffset) {
          currentTPOffset.put(tp, eOffset)
          println("Offset头越界  topic[" + topic + "] patition [" + i + "] fromOffsets [" + partitionOffset + "] earlistOffset [" + eOffset + "]")
          log.warn("Offset头越界  topic[" + topic + "] patition [" + i + "] fromOffsets [" + partitionOffset + "] earlistOffset [" + eOffset + "]")
        } else if (pOffset > lOffset) {
          if ("latest".equals(reset)) {
            currentTPOffset.put(tp, lOffset)
          } else {
            currentTPOffset.put(tp, eOffset)
          }
          println("Offset头越界  topic[" + topic + "] patition [" + i + "] fromOffsets [" + partitionOffset + "] latestOffset [" + lOffset + "]")
          log.warn("Offset头越界  topic[" + topic + "] patition [" + i + "] fromOffsets [" + partitionOffset + "] latestOffset [" + lOffset + "]")
        } else {
          currentTPOffset.put(tp, pOffset)
        }
      }
    }

    System.out.println("earlistTPOffset: " + earlistTPOffset.toString)
    System.out.println("latestTPOffset: " + latestTPOffset.toString)
    System.out.println("currentTPOffset: " + currentTPOffset.toString)

    log.info("earlistTPOffset: " + earlistTPOffset.toString)
    log.info("latestTPOffset: " + latestTPOffset.toString)
    log.info("currentTPOffset: " + currentTPOffset.toString)

    var fromOffsets: scala.collection.mutable.Map[TopicPartition, Long] = null
    if (isNewApp || !upgraded) {
      if ("latest".equals(reset)) {
        fromOffsets = JavaConversions.mapAsScalaMap(latestTPOffset)
      } else {
        fromOffsets = JavaConversions.mapAsScalaMap(earlistTPOffset)
      }
      System.out.println("isNewApp: " + isNewApp + " | upgraded: " + upgraded)
      log.info("isNewApp: " + isNewApp + " | upgraded: " + upgraded)
    } else {
      fromOffsets = JavaConversions.mapAsScalaMap(currentTPOffset)
    }
    System.out.println("fromOffsets: " + fromOffsets.toString())
    log.info("fromOffsets: " + fromOffsets.toString())

    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams, fromOffsets))
    stream
  }

  def createDirectStream(ssc: StreamingContext, kafkaParams: Map[String, Object], topics: Array[String], zkHosts: String, upgraded: Boolean):InputDStream[ConsumerRecord[String, String]] = {
    createDirectStream(ssc,kafkaParams,topics,zkHosts,upgraded,"/kafkaOffset")
  }
  def createDirectStream(ssc: StreamingContext, kafkaParams: Map[String, Object], topics: Array[String], zkHosts: String):InputDStream[ConsumerRecord[String, String]] = {
    createDirectStream(ssc,kafkaParams,topics,zkHosts,true,"/kafkaOffset")
  }

  def createDirectStreamFromHDFS(ssc:StreamingContext,kafkaParams:Map[String, String],topics: Array[String],properties:CaseConf):InputDStream[ConsumerRecord[String, String]]={
    var stream:InputDStream[ConsumerRecord[String, String]] = null
    val offsets: Array[OffsetRange] = getOffSetFromHDFS(properties.get("framework.streaming.offsetSavePath"), new Configuration())
    var consumerOffsets:Map[TopicPartition,Long] = Map()
    if(null != offsets){
      for(offset <- offsets){
        val topicPartition = new TopicPartition(offset.topic,offset.partition)
        consumerOffsets += (topicPartition -> offset.untilOffset)
      }
    }

    if(consumerOffsets.size > 0){
     stream = KafkaUtils.createDirectStream[String,String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams, consumerOffsets))
    } else {
      stream = KafkaUtils.createDirectStream[String,String](ssc,PreferConsistent,Subscribe[String,String](topics,kafkaParams))
    }
    stream
  }

  def saveOffset(offsetRanges:Array[OffsetRange],broadcastedConf:Broadcast[CaseConf]): Unit ={
    val properties: CaseConf = broadcastedConf.value

    zkInit(properties)
    val zkPath = s"${properties.get("framework.streaming.zk.path","/kafkaOffset")}/${properties.get("framework.streaming.appName")}}"
    for(o <- offsetRanges){
      val path = s"${zkPath}/${o.topic}/${o.partition}"
      updatePersistentPath(zkClient,path,o.untilOffset.toString)
    }
  }

  def saveOffset(offsetRanges:Array[OffsetRange],properties:CaseConf): Unit ={

    zkInit(properties)
    val zkPath = s"${properties.get("framework.streaming.zk.path","/kafkaOffset")}/${properties.get("framework.streaming.appName")}}"
    for(o <- offsetRanges){
      val path = s"${zkPath}/${o.topic}/${o.partition}"
      updatePersistentPath(zkClient,path,o.untilOffset.toString)
    }
  }

  def saveOffset(offsetRanges:Array[OffsetRange],zkHosts:String,offSerRootPath:String,appName:String): Unit ={

    zkInit(zkHosts)
    val zkPath = s"${offSerRootPath}/${appName}}"
    for(o <- offsetRanges){
      val path = s"${zkPath}/${o.topic}/${o.partition}"
      updatePersistentPath(zkClient,path,o.untilOffset.toString)
    }
  }


  def updatePersistentPath(client: ZkClient,path:String,data:String)={
    try{
      client.writeData(path,data)
    } catch {
      case e:ZkNoNodeException =>{
        val parentDir: String = path.substring(0,path.lastIndexOf("/"))
        if(parentDir.length !=0){
          client.createPersistent(parentDir,true)
        }

         try{
           client.createPersistent(path,data)
         } catch {
           case e:ZkNoNodeException =>{
             client.writeData(path,data)
           }
           case e2:Throwable => throw e2
         }
      }
      case e2:Throwable => throw e2
    }
  }

  def saveOffSetsToHDFS(offSets:Array[OffsetRange],savePath:String,conf:Configuration)={
    val saveArray = new ArrayBuffer[String]()
    for (offset <- offSets) {
      val offSetStr = offset.topic + "," + offset.partition + "," +offset.fromOffset + "," +offset.untilOffset
      saveArray += offSetStr
    }
    HdfsUtil.saveTextFileToHdfs(saveArray.toArray,savePath,conf,false)
  }


  def getOffSetFromHDFS(hdfsPath:String,conf:Configuration):Array[OffsetRange]={
    val offsetRanges = new ArrayBuffer[OffsetRange]()
    val fs: FileSystem = FileSystem.get(URI.create(hdfsPath), conf)
    val path = new Path(hdfsPath)

    if(!fs.exists(path)){
      return null
    }

    val hdfsInStream: FSDataInputStream = fs.open(path)
    val br = new BufferedReader(new InputStreamReader(hdfsInStream))
    var line: String = br.readLine()
    while( null != line && !"".equals(line)){
      val offStr: Array[String] = line.split(",")
      val topic: String = offStr(0)
      val partition: Int = offStr(1).toInt
      val fromOffset: Long = offStr(2).toLong
      val untilOffset: Long = offStr(3).toLong
      val offsetRange: OffsetRange = OffsetRange.create(topic, partition, fromOffset, untilOffset)
      offsetRanges += offsetRange
      line = br.readLine
    }
    offsetRanges.toArray
  }

}
