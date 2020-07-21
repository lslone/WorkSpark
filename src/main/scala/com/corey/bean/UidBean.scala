package com.corey.bean

import com.google.gson.annotations.SerializedName

class UidBean(ouid:String)extends Serializable {
  @SerializedName("UID")
  val uid: String =ouid

}
