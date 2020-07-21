package com.corey.streamingframework.Utils

import java.io.{File, PrintWriter}

import com.corey.streamingframework.security.SecurityUtils
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.streaming.kafka010.OffsetRange

import scala.collection.mutable.ArrayBuffer
import scala.io.{BufferedSource, Source}

object OffsetMoveUtil {

  def readZKOffset(zkHosts:String,appName:String,topicsStr:String,offSetRootPath:String):Array[OffsetRange]={
    val zkPath = s"${offSetRootPath}/${appName}"
    val topics: Array[String] = topicsStr.split(",")
    val offsetBuff = new ArrayBuffer[OffsetRange]()
    val zkClient: ZkClient = ZkUtils.createZkClient(zkHosts,18000,18000)
    System.out.println("********************"+ zkHosts + "*************************")
    for(topic <-topics){
      val topicPath = s"${zkPath}/$topic"
      val children: Int = zkClient.countChildren(topicPath)

      if (children == 0){
        System.out.println("没有花去到offset，请确认zkhost、topic、path、appname是否书写正确")
      }

      for(i <- 0 until children){
        val partitonOffset: String = zkClient.readData[String](s"${topicPath}/$i")
        val pOffset: Long = partitonOffset.toLong
        val offset: OffsetRange = OffsetRange.create(topic,i,0,pOffset)
        System.out.println("topic:" + topic + "  partition:" + i + "   fromOffset" + 0 + "   untilOffset:" + pOffset)
        offsetBuff.append(offset)
      }
    }
    System.out.println("********************"+ zkHosts + "*************************")
    offsetBuff.toArray
  }

  def saveOffsetToLocal(offsets:Array[OffsetRange],savePath:String)={
    val saveArray = new ArrayBuffer[String]()
    for(offSet <- offsets){
      val offSetStr = offSet.topic + "," + offSet.partition + "," + offSet.fromOffset + ","  + offSet.untilOffset
      saveArray += offSetStr
    }

    val writer = new PrintWriter(new File(savePath))
    for(line <- saveArray){
      writer.write(line +"\n")
    }
    writer.close()
  }

  def getOffsetFromLocal(filePath:String):Array[OffsetRange]={
    val offsetRanges = new ArrayBuffer[OffsetRange]()
    val file: BufferedSource = Source.fromFile(filePath)
    val lines: Iterator[String] = file.getLines()
    for (line <- lines){
      val offStr: Array[String] = line.split(",")
      val topic: String = offStr(0)
      val partition: Int = offStr(1).toInt
      val fromOffset: Long = offStr(2).toLong
      val untilOffset: Long = offStr(3).toLong
      val offsetRange: OffsetRange = OffsetRange.create(topic,partition,fromOffset,untilOffset)
      offsetRanges.append(offsetRange)
    }
    file.close()
    offsetRanges.toArray
  }

  def main(args: Array[String]): Unit = {
    if(args.length <= 0){
      System.out.println("args is null")
      System.exit(1)
    }

    val conf = new CaseConf(args(0))
    val appName: String = conf.get("appName")
    val offSetRootPath: String = conf.get("offSetRootPath", "/kafkaOffset")
    val topicsStr: String = conf.get("topicsStr")
    val sourceZKHost: String = conf.get("source.zk.hosts")
    val targetZKHost: String = conf.get("target.zk.hosts")
    val mode: String = conf.get("option.mode", "read")

    val isSASL: Boolean = conf.get("issasl", "true").toBoolean

    if(!isSASL){
      var userKeytabFile: String = conf.get("source.userKeytabFile")
      var krb5File: String = conf.get("source.krb5File")
      var userName: String = conf.get("source.userName")

      if(!"read".equalsIgnoreCase(mode)){
        userKeytabFile = conf.get("target.userKeytabFile")
        krb5File= conf.get("target.krb5File")
        userName= conf.get("target.userName")
      }

      System.out.println("userKeytabFile  krb5File  userName ：" + userKeytabFile + " | " + krb5File + " | " + userName + "\n")
      SecurityUtils.login(userKeytabFile,krb5File,userName)
    }

    var path: String = this.getClass.getProtectionDomain.getCodeSource.getLocation.getPath
    val firstIndex: Int = path.lastIndexOf(System.getProperty("path.separator")) +1
    val lastIndex: Int = path.lastIndexOf(File.separator) +1
    path = path.substring(firstIndex, lastIndex)

    val savePath = path + "offset_" + appName + ".txt"

    if("read".equalsIgnoreCase(mode)){
      val offsetRanges: Array[OffsetRange] = readZKOffset(sourceZKHost, appName, topicsStr, offSetRootPath)
      System.out.println("Offset 读取完毕！\n")
      System.out.println("offset 写入本地文件开始！savePath: " + savePath)
      saveOffsetToLocal(offsetRanges,savePath)
      System.out.println("offset 写入本地文件完成！")
    } else {
      val offsetRanges: Array[OffsetRange] = getOffsetFromLocal(savePath)
      System.out.println("offset 写入新zk开始！")
      StreamingUtils.saveOffset(offsetRanges,targetZKHost,offSetRootPath,appName)
      System.out.println("offset 写入新zk，开始读取新zk的offset,请注意校验")
      readZKOffset(targetZKHost,appName,topicsStr,offSetRootPath)
    }

  }

}
