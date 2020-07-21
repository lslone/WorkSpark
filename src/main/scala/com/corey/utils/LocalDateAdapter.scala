package com.corey.utils

import java.lang.reflect.Type
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.google.gson._
import org.slf4j.LoggerFactory

class LocalDateAdapter  extends JsonSerializer[LocalDate] with JsonDeserializer[LocalDate] {
  val inputTime=DateTimeFormatter.ofPattern("yyyyMMdd")
  val outputTime=DateTimeFormatter.ofPattern("yyyy-MM-dd")
  private val LOG = LoggerFactory.getLogger(getClass)
  override def serialize(src: LocalDate, typeOfSrc: Type, context: JsonSerializationContext): JsonElement = {
    new JsonPrimitive(src.format(outputTime))
  }

  override def deserialize(json: JsonElement, typeOfT: Type, context: JsonDeserializationContext): LocalDate = {
    try {
      val decimal = json.getAsString
      LocalDate.parse(decimal,inputTime)
    } catch {
      case e:Exception =>
        LOG.error(s"Message[$json] could not parse to [$typeOfT] because of:${e.getMessage}")
        null
    }
  }

}
