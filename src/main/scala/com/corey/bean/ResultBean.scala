package com.corey.bean

import com.google.gson.annotations.SerializedName

class ResultBean(ab:String,as:String,aen:String,ad:DBean) extends Serializable {
  @SerializedName("b")
  val b: String =ab

  @SerializedName("s")
  val s: String =as

  @SerializedName("en")
  val en: String =aen

  @SerializedName("d")
  val d: DBean = ad
}

class DBean(aoid:String) extends Serializable{
  @SerializedName("oid")
  val oid: String = aoid
}
