package com.corey.utils


import java.lang.reflect.Type

import com.google.gson._
import org.slf4j.LoggerFactory

class BigdecimalAdapter extends JsonSerializer[BigDecimal] with JsonDeserializer[BigDecimal]{
  private val LOG = LoggerFactory.getLogger(getClass)
  override def serialize(src: BigDecimal, typeOfSrc: Type, context: JsonSerializationContext): JsonElement = {
    new JsonPrimitive(src.bigDecimal)
  }

  override def deserialize(json: JsonElement, typeOfT: Type, context: JsonDeserializationContext): BigDecimal = {
    try {
      json.getAsBigDecimal
    } catch {
      case e:Exception =>
        LOG.error(s"Message[$json] could not parse to [$typeOfT] because of:${e.getMessage}")
      null
    }
  }

}
