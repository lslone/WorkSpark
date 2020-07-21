package com.corey.streamingframework.busi

import com.corey.streamingframework.Utils.CaseConf
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

trait BusinessProcess {
  /**
    * 设置sparkConf参数
    * @param sparkConf SparkConf实例
    * @param caseConf 配置文件实例
    * @return
    */
  def addConf(sparkConf:SparkConf,caseConf:CaseConf):SparkConf={
    sparkConf
  }

  /**
    * 初始化sc
    * @param sc
    * @param caseConf
    * @return
    */
  def initSC(sc:SparkContext,caseConf: CaseConf):SparkContext={
    sc
  }

  /**
    * 业务逻辑实现
    * @param rdd
    * @param caseConf
    */
  def busProcessImp(rdd:RDD[String],caseConf: CaseConf)

}
