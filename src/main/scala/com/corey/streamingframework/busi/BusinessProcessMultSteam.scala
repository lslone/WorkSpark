package com.corey.streamingframework.busi

import com.corey.streamingframework.Utils.CaseConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.{SparkConf, SparkContext}

trait BusinessProcessMultSteam {
  def addConf(sparkConf: SparkConf,caseConf: CaseConf):SparkConf={
    sparkConf
  }

  def initSC(sc:SparkContext,caseConf: CaseConf):SparkContext={
    sc
  }

  def createStream(ssc:StreamingContext,caseConf: CaseConf)

}
