package com.corey.bean

import java.time.LocalDateTime

import com.corey.utils.JsonUtils
import com.google.gson.annotations.SerializedName


class OutBean (atimestamp:String,aclient_ip:String,amethod:String,ab:String,as:String,aen:String,aoid:String)extends Serializable {
  @SerializedName("timestamp")
  val timestamp: String = atimestamp

  @SerializedName("client_ip")
  val client_ip: String = aclient_ip

  @SerializedName("method")
  val method: String = amethod

  @SerializedName("b")
  val b: String =ab

  @SerializedName("s")
  val s: String =as

  @SerializedName("en")
  val en: String =aen

  @SerializedName("oid")
  val oid: String = aoid

  def  toJson:String={
    JsonUtils.toJson(this)
  }

}
