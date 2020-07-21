package com.corey.bean

import com.google.gson.annotations.SerializedName

class SourceBean (aMessage:String) extends Serializable {

  @SerializedName("message")
  val message: String = aMessage


}

class MsgBean(atimestamp:String,aclient_ip:String,amethod:String,aargs:String) extends Serializable{

  @SerializedName("timestamp")
  val timestamp: String = atimestamp

  @SerializedName("client_ip")
  val client_ip: String = aclient_ip

  @SerializedName("method")
  val method: String = amethod

  @SerializedName("args")
  val args: String = aargs

}
