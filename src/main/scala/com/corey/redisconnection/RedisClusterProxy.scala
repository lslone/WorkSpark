package com.corey.redisconnection

import org.slf4j.{Logger, LoggerFactory}
import redis.clients.jedis.JedisCluster

import scala.collection.mutable.ArrayBuffer

class RedisClusterProxy extends Serializable {
  private val LOG: Logger = LoggerFactory.getLogger(getClass)
  private var redisMap: Map[String, RedisOperations] = Map[String, RedisOperations]()
  val redisServerList: ArrayBuffer[(String, String, String)] = ArrayBuffer[(String, String, String)]()

  def closeAll(): Unit = {
    redisMap.foreach(redis => redis._2.close())
  }

  def getService(redisName: String): RedisOperations = {
    if (redisMap.get(redisName).isDefined && redisMap.get(redisName).get.ALIVE) {
      redisMap.get(redisName).get
    }
    else {
      redisMap += (redisName -> create(redisName))
      redisMap.get(redisName).get
    }
  }

  def createJedisCluster(redisName: String): JedisCluster = {
    var conn: JedisCluster = null
    redisServerList.foreach(server => {
      if (server._1.equals(redisName)) {
        val startTime = System.currentTimeMillis()
        conn = JedisClusterFactory.create(server._2.split("&&")(0), if (server._2.split("&&").size > 1)
          server._2.split("&&")(1) else "")
        val totalTime = System.currentTimeMillis() - startTime
        println(s"创建${redisName}连接：${totalTime}")
        LOG.info(s"创建${redisName}连接：${totalTime}")
      }
    })
    conn
  }

  private def getRedisServerList() = {
    ConfigUtil.REDIS_SERVER.split(":").foreach(server => {
      val serverInfo = server.split("=>")
      val inAndPassword = serverInfo(1).split("/")
      redisServerList.+=:(serverInfo(0), inAndPassword(0), if (inAndPassword.size > 1) inAndPassword(1) else "")
    })
  }

  def create(redisName: String): RedisOperations = {
    try {
      val redisOperation = new RedisOperationsImpl(redisName, createJedisCluster(redisName))
      redisOperation.EXCEPTION_FLAG = false
      redisOperation.ALIVE = true

      redisOperation
    } catch {
      case e: Exception => throw new Exception()
    }
  }
}

object RedisClusterProxy {
  def apply(): RedisClusterProxy = {
    try {
      val proxy = new RedisClusterProxy()
      proxy.getRedisServerList()
      proxy.redisServerList.foreach(server => proxy.redisMap += (server._1 -> proxy.create(server._1)))
      proxy
    } catch {
      case e: Exception => throw new Exception()
    }
  }

}


