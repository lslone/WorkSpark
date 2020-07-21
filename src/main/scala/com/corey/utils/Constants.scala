package com.corey.utils

object Constants {

  final val CHECKPOINT_ROOT_PATH="framework.streaming.checkpointDir"

  final val APP_NAME="framework.steaming.appName"

  final val INTERVAL="framework.streaming.banchTime"

  val GROUP_ID="framework.input.group"

  val AUTO_RESET_OFFSET="framework.streaming.auto.offset.reset"

  final val CONSUMER_BOOTSTRAP_SERVERS ="framework.input.brokerlist"

  val SORCE_TOPIC="framework.input.topics"

  val TARGET_EVENT_ID="target.event.id"

  val PRODUCER_BOOTSTRAP_SERVERS="framework.output.brokerlist"
  val TARGET_TOPIC="framework.output.topics"

//  重分区
  val REPARTITION="rdd.need.coalesce"
  val REPARTITION_NUM="coalesce.numPartitions"



}
