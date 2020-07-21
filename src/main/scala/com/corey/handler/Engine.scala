package com.corey.handler

import java.text.MessageFormat

import com.corey.redisconnection.RedisClusterFactory
import com.corey.streamingframework.Utils.{CaseConf, StreamingUtils}
import com.corey.utils.Constants
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions.mapAsJavaMap

class Engine[T] extends Serializable {
  private val LOG = LoggerFactory.getLogger(getClass)

  def run(args: Array[String], handler: Handler[T]): Unit = {
    if (args.length < 1) {
      LOG.error(s"args prop file path is null!")
      System.exit(1)
    }
    val caseConf = new CaseConf(args(0))
    val kafkaCousumerServers = caseConf.get(Constants.CONSUMER_BOOTSTRAP_SERVERS)

    val kafkaProducerServers = caseConf.get(Constants.PRODUCER_BOOTSTRAP_SERVERS)

    val chechpointRoot = caseConf.get(Constants.CHECKPOINT_ROOT_PATH)
    val autoResetoffset = caseConf.get(Constants.AUTO_RESET_OFFSET, "latest")
    val targetTopic = caseConf.get(Constants.TARGET_TOPIC)
    val groupId = caseConf.get(Constants.GROUP_ID)
    val interval = Seconds(caseConf.get(Constants.INTERVAL, "30").toInt)
    val sourceTopics = caseConf.get(Constants.SORCE_TOPIC).split(",")
    val coalesce = caseConf.get(Constants.REPARTITION)
    val partitionNum = caseConf.get(Constants.REPARTITION_NUM)


    val checkpointPath = s"$chechpointRoot"
    val ssc = StreamingContext.getOrCreate(checkpointPath, () => {
      handler.createContext(checkpointPath, interval)
    })
    var kafkaConsumerParams = Map[String, Object](
      "bootstrap.servers" -> kafkaCousumerServers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "key.serializer" -> classOf[StringSerializer],
      "value.serializer" -> classOf[StringSerializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> autoResetoffset,
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    var kafkaProducerParams = Map[String, Object](
      "bootstrap.servers" -> kafkaProducerServers,
      "key.serializer" -> classOf[StringSerializer],
      "value.serializer" -> classOf[StringSerializer],
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer]
    )

    val kafkaStream = KafkaUtils.createDirectStream[String,String](ssc, LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String,String](sourceTopics,kafkaConsumerParams ))

    kafkaStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val coalesceRdd = if (coalesce.toBoolean) rdd.coalesce(partitionNum.toInt) else rdd
        val trans = coalesceRdd.map(x =>
          (x.key(), x.value())).flatMap(msg => handler.parseMsg(msg))
        var producer: KafkaProducer[String, String] = null

        trans.foreachPartition(partition => {
          if (!partition.isEmpty) {
            println("hahahha")
            producer = new KafkaProducer[String, String](kafkaConsumerParams)
            try {
              partition.foreach(record => {
                //TODO 真正执行的方法
                handler.proceduce(record._2, targetTopic, producer)
              })
            } finally {
              producer.close()
            }
          }
        })
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        StreamingUtils.saveOffset(offsetRanges, caseConf)
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }


}
