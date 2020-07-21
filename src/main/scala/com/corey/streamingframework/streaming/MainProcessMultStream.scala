package com.corey.streamingframework.streaming
import com.corey.streamingframework.Utils.CaseConf
import com.corey.streamingframework.busi.{BusinessProcess, BusinessProcessMultSteam}
import com.corey.streamingframework.streaming.MainProcess.{createContext, process, processLog}
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

object MainProcessMultStream {
  private val processLog: Log = LogFactory.getLog(MainProcess.getClass)

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("args prop file path is null!")
      System.exit(1)
    }

    var needAuth = false

    if (args.length > 1 && args(2).equalsIgnoreCase("true")) {
      needAuth = true
    }

    init(args(0), needAuth)
  }

  def init(confPath: String, needAuth: Boolean): Unit = {
    try {
      val caseConf = new CaseConf(confPath)
      val checkpointDir: String = caseConf.get("framework.streaming.checkpointDir")
      val appName: String = caseConf.get("framework.streaming.appName", "MyApp")
      val banchTime: Int = caseConf.get("framework.streaming.banchTime", "10").toInt

      val ssc: StreamingContext = StreamingContext.getOrCreate(checkpointDir, () => {
        createContext(appName, banchTime, checkpointDir, caseConf, needAuth)
      })

      ssc.start()
      ssc.awaitTermination()
    } catch {
      case e: Exception => {
        processLog.error(e.getMessage, e)
        println("error: " + e.getMessage)
      }
    }
  }

  def createContext(appName: String, banchTime: Int, checkpointDir: String, caseConf: CaseConf, needAuth: Boolean): StreamingContext = {
    val sparkConf: SparkConf = new SparkConf().setAppName(appName)

    val master = sparkConf.get("spark.master","")
    if("".equals(master)){
      sparkConf.setMaster("local[*]")
      processLog.warn("Master is not set , will be local[*]")
    }
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerialzer")
    val arr = ArrayBuffer[Class[_]]()
    arr.append(classOf[CaseConf])

    val needKryoSerClassName: String = caseConf.get("framework.streaming.needKryoSerClassName", "").trim

    if (!"".equals(needKryoSerClassName)) {
      val classNames: Array[String] = needKryoSerClassName.split(",")
      for (elem <- classNames) {
        arr.append(Class.forName(elem))
      }
    }

    val busImpClassName: String = caseConf.get("framework.streaming.busImpMultStream").trim

    val pro = Class.forName(busImpClassName).newInstance().asInstanceOf[BusinessProcessMultSteam]
    sparkConf.registerKryoClasses(arr.toArray)

    val sparkConf1: SparkConf = pro.addConf(sparkConf, caseConf)
    val sc = new SparkContext(sparkConf1)
    val sc1: SparkContext = pro.initSC(sc, caseConf)

    val ssc = new StreamingContext(sc1, Seconds(banchTime))
    ssc.checkpoint(checkpointDir)

    try {
      pro.createStream(ssc,caseConf)
    } catch {
      case e: Exception =>
        processLog.error(e.getMessage, e)
    }

    ssc
  }
}
