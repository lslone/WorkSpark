package com.corey.streamingframework.Utils

import java.io.{FileInputStream, FileNotFoundException, InputStreamReader}
import java.util
import java.util.Properties

import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}

import scala.collection.mutable

class CaseConf(propertiesPath:String) extends Serializable {
  private val log: Log = LogFactory.getLog(classOf[CaseConf])

  private val setting = new mutable.HashMap[String,String]()

  load(propertiesPath)

  def load(propertiesPath:String): Unit ={
    loadProPertiesFile(propertiesPath)
  }

  def loadProPertiesFile(propertiesPath:String): Unit ={
    var in:FSDataInputStream = null
    var inr:InputStreamReader = null

    try{
      val conf = new Configuration()
      val path = new Path(propertiesPath)
      val fs: FileSystem = path.getFileSystem(conf)

      in = fs.open(path)
      val prop = new Properties()

      inr = new InputStreamReader(in,"utf-8")
      prop.load(inr)

      val keys: util.Enumeration[_] = prop.propertyNames()

      while (keys.hasMoreElements){
        val key: String = keys.nextElement().toString
        setting += ((key,prop.getProperty(key).trim))
      }
    } catch {
      case e:FileNotFoundException=>{
        println("#"*30)
        println(e.getMessage)
        log.error(e.getMessage,e)
        println("#"*30)
      }
    } finally {
      if(inr != null){
        inr.close()
      }
      if(in!= null){
        in.close()
      }
    }

    println("#"*30)
    setting.foreach(println)
    println("#"*30)
  }

  def loadLocal(propertiesPath:String): Unit ={
    var in:FileInputStream = null
    try{
      in = new FileInputStream(propertiesPath)
      val prop = new Properties()
      prop.load(in)

      val keys: util.Enumeration[_] = prop.propertyNames()

      while (keys.hasMoreElements){
        val key: String = keys.nextElement().toString
        setting += ((key,prop.getProperty(key).trim))
      }

    }catch {
      case e:FileNotFoundException=>{
        println("#"*30)
        println(e.getMessage)
        log.error(e.getMessage,e)
        println("#"*30)
      }
    } finally {
      if(in!= null){
        in.close()
      }
    }

    println("#"*30)
    setting.foreach(println)
    println("#"*30)
  }

  def get(key:String):String={
    setting.getOrElse(key,throw new NoSuchElementException(key))
  }

  def get(key:String,defaultValue:String):String={
    setting.getOrElse(key,defaultValue)
  }

  def getAll:Map[String,String]={
    setting.toMap
  }

  def getOption(key:String):Option[String]={
    setting.get(key)
  }

  def getOption(key:String,defaultValue:Int):Int={
    getOption(key).map(_.toInt).getOrElse(defaultValue)
  }

  def getLong(key:String,defaultValue:Long):Long={
    getOption(key).map(_.toLong).getOrElse(defaultValue)
  }

  def getDouble(key:String,defaultValue:Double):Double={
    getOption(key).map(_.toDouble).getOrElse(defaultValue)
  }
}
