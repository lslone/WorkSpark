package com.corey.utils

import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter

import com.google.gson.{GsonBuilder, JsonObject}
import org.slf4j.LoggerFactory


object JsonUtils {
  private val LOG = LoggerFactory.getLogger(getClass)
  private val gson = (new GsonBuilder).create()
//    .registerTypeAdapter(classOf[LocalDate],new LocalDateAdapter)
//    .registerTypeAdapter(classOf[LocalDateTime],new LocalDateTimeAdapter)
//    .registerTypeAdapter(classOf[BigDecimal],new BigdecimalAdapter)
//    .create()

  def toJson[T](bean:T):String={
    gson.toJson(bean)
  }

  def fromJson[T](jsonObject: String, value: Class[T]):Option[T] = {
    try {
      Some(gson.fromJson(jsonObject, value))
    } catch {
      case e:Exception =>{
        LOG.error(s"Message[$jsonObject] could not parse to [$value] because of:${e.getMessage}")
        None
      }
    }

  }

  def addTimeJson(value:String):String={
    val timeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val jsonObject: JsonObject = fromJson(value,classOf[JsonObject]).get
    jsonObject.addProperty("DEATIM",LocalDateTime.now().format(timeFormatter))
    jsonObject.toString
  }
}
