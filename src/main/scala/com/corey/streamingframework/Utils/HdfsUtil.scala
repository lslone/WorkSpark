package com.corey.streamingframework.Utils

import java.io.{BufferedWriter, IOException, OutputStreamWriter}

import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object HdfsUtil {

  private val log = LogFactory.getLog(HdfsUtil.getClass)

  def saveTextFileToHdfs(contents:Array[String],outputPath:String,conf:Configuration,isAppend:Boolean): Unit ={
    var writer:BufferedWriter = null
    try{
      val fs: FileSystem = FileSystem.get(conf)
      val path = new Path(outputPath)
      if(isAppend){
        if(!fs.exists(path)){
          fs.createNewFile(path)
        }
        writer = new BufferedWriter(new OutputStreamWriter(fs.append(path)))
      } else {
        writer = new BufferedWriter(new OutputStreamWriter(fs.create(path)))
      }
      for(content <- contents){
        writer.write(content +"\n")
      }

    } catch {
      case e:Exception=>{
        println("##################")
        println(e.toString)
        log.error(e.toString,e)
        println("##################")
      }
      case e:IOException =>{
        println("##################")
        println(e.toString)
        log.error(e.toString,e)
        println("##################")
      }
    } finally {
      if(writer != null){
        writer.close()
      }
    }
  }
}
