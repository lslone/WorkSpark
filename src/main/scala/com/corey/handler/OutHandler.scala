package com.corey.handler

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, LocalTime}

import com.corey.bean.{MsgBean, OutBean, ResultBean, SourceBean, UidBean}
import com.corey.redisconnection.RedisClusterFactory
import com.corey.utils.{Constants, JsonUtils}
import org.apache.commons.lang.StringUtils
import org.apache.kafka.clients.producer.KafkaProducer
import org.slf4j.LoggerFactory

import scala.math.BigDecimal.RoundingMode

class OutHandler extends Handler[SourceBean] {
  override val tBeanClass: Class[SourceBean] = classOf[SourceBean]
  private val LOG = LoggerFactory.getLogger(getClass)

  /**
    * 程序步骤
    */
  override def proceduce(msg: SourceBean, targeTopic: String, producer: KafkaProducer[String, String]): Unit = {

    val message: String = msg.message
    val msgBean: MsgBean = JsonUtils.fromJson(message,classOf[MsgBean]).get
    val x: String = msgBean.args

    val args: String = x.replaceAll("params=", "")
    val result = java.net.URLDecoder.decode(args, "UTF-8")

    val resultBean: ResultBean = JsonUtils.fromJson(result,classOf[ResultBean]).get

    val outValue: String = new OutBean(msgBean.timestamp, msgBean.client_ip, msgBean.method, resultBean.b, resultBean
      .s, resultBean.en, resultBean.d.oid).toJson

    sendMsg("uid",targeTopic,producer,outValue)
  }


}
