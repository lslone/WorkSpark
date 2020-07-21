package com.corey.streamingframework.logger

import org.apache.log4j.Logger


object Log {

  def debug(clazz:Class[_],appName:String,msg:String)={
    Logger.getLogger(clazz).debug("["+appName+"]"+msg)
  }
  def debug(clazz:Class[_],appName:String,msg:String,e:Throwable)={
    Logger.getLogger(clazz).debug("["+appName+"]"+msg,e)
  }
  def info(clazz:Class[_],appName:String,msg:String)={
    Logger.getLogger(clazz).info("["+appName+"]"+msg)
  }
  def info(clazz:Class[_],appName:String,msg:String,e:Throwable)={
    Logger.getLogger(clazz).info("["+appName+"]"+msg,e)
  }
  def warn(clazz:Class[_],appName:String,msg:String)={
    Logger.getLogger(clazz).warn("["+appName+"]"+msg)
  }
  def warn(clazz:Class[_],appName:String,msg:String,e:Throwable)={
    Logger.getLogger(clazz).warn("["+appName+"]"+msg,e)
  }
  def error(clazz:Class[_],appName:String,msg:String)={
    Logger.getLogger(clazz).error("["+appName+"]"+msg)
  }
  def error(clazz:Class[_],appName:String,msg:String,e:Throwable)={
    Logger.getLogger(clazz).error("["+appName+"]"+msg,e)
  }
  def fatal(clazz:Class[_],appName:String,msg:String)={
    Logger.getLogger(clazz).fatal("["+appName+"]"+msg)
  }
  def fatal(clazz:Class[_],appName:String,msg:String,e:Throwable)={
    Logger.getLogger(clazz).fatal("["+appName+"]"+msg,e)
  }


}
