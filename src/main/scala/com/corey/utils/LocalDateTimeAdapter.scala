package com.corey.utils

import java.lang.reflect.Type
import java.time. LocalDateTime
import java.time.format.DateTimeFormatter

import com.google.gson._
import org.slf4j.LoggerFactory

class LocalDateTimeAdapter  extends JsonSerializer[LocalDateTime] with JsonDeserializer[LocalDateTime] {
  val inputTime=DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")
  val outputTime=DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
  private val LOG = LoggerFactory.getLogger(getClass)
  override def serialize(src: LocalDateTime, typeOfSrc: Type, context: JsonSerializationContext): JsonElement = {
    new JsonPrimitive(src.format(outputTime))
  }

  override def deserialize(json: JsonElement, typeOfT: Type, context: JsonDeserializationContext): LocalDateTime = {
    try {
      val decimal = json.getAsString
      LocalDateTime.parse(decimal,inputTime)
    } catch {
      case e:Exception =>
        LOG.error(s"Message[$json] could not parse to [$typeOfT] because of:${e.getMessage}")
        null
    }
  }

}
