package com.corey.redisconnection

import java.lang

import org.slf4j.{Logger, LoggerFactory}
import redis.clients.jedis.exceptions.{JedisConnectionException, JedisDataException}
import redis.clients.jedis.{JedisCluster, ScanParams, ScanResult, Tuple}

import scala.collection.JavaConversions.{asScalaBuffer,asScalaSet,mapAsJavaMap,mapAsScalaMap}

class RedisOperationsImpl(redisName: String, cluster: JedisCluster) extends RedisOperations with Serializable {

 private val LOG: Logger = LoggerFactory.getLogger(getClass)

  private def handleException(ex:Exception):Boolean={
if(ex!=null){
  EXCEPTION_FLAG=true
  ex match {
    case _ :JedisConnectionException =>{
      ALIVE =false
      LOG.error("redis ["+ redisName +"] lost ",ex)
      println("redis ["+ redisName +"] lost ",ex)
    }
    case  _ :JedisDataException=> {
      if (ex.getMessage != null && ex.getMessage.indexOf("READONLY")!= -1) {
      LOG.error("redis ["+ redisName +"] are read-only slave.",ex)
      }else{
        EXCEPTION_FLAG = false
      }
    }
    case _ =>{
      ALIVE = false
      ex.printStackTrace()
    }
  }
}
    EXCEPTION_FLAG
  }
  override def expire(key: String, expireSeconds: Int): Long = {
    try {
      cluster.expire(key, expireSeconds)
    } catch {
      case ex:Exception =>
        EXCEPTION_FLAG =handleException(ex)
        throw ex
    } finally {
      if(EXCEPTION_FLAG)close()
    }
  }

  override def ttl(key: String): Long = {
    try {
      cluster.ttl(key)
    } catch {
      case ex:Exception =>
        EXCEPTION_FLAG =handleException(ex)
        throw ex
    } finally {
      if(EXCEPTION_FLAG)close()
    }
  }

  override def get(key: String): String = {
    try {
      cluster.get(key)
    } catch {
      case ex:Exception =>
        EXCEPTION_FLAG =handleException(ex)
        throw ex
    } finally {
      if(EXCEPTION_FLAG)close()
    }
  }

  override def del(key: String): Long = {
    try {
      cluster.del(key)
    } catch {
      case ex:Exception =>
        EXCEPTION_FLAG =handleException(ex)
        throw ex
    } finally {
      if(EXCEPTION_FLAG)close()
    }
  }

  override def set(key: String, value: String): String = {
    try {
      cluster.set(key, value)
    } catch {
      case ex:Exception =>
        EXCEPTION_FLAG =handleException(ex)
        throw ex
    } finally {
      if(EXCEPTION_FLAG)close()
    }
  }

  override def set(key: String, value: String, nxxx: String, expx: String, time: Long): String = {
    try {
      cluster.set(key,value,nxxx,expx,time)
    } catch {
      case ex:Exception =>
        EXCEPTION_FLAG =handleException(ex)
        throw ex
    } finally {
      if(EXCEPTION_FLAG)close()
    }
  }

  override def zaddBatch(key: String, values: Iterable[(String, Double)]): Long = {
    try {
      val value = values.map(r=>(r._1,new lang.Double(r._2)))
      cluster.zadd(key, value.toMap)
    } catch {
      case ex:Exception =>
        EXCEPTION_FLAG =handleException(ex)
        throw ex
    } finally {
      if(EXCEPTION_FLAG)close()
    }
  }

  override def zremRangeByScore(key: String, minScore: Double, maxScore: Double): Long = {
    try {
      cluster.zremrangeByScore(key,minScore,maxScore)
    } catch {
      case ex:Exception =>
        EXCEPTION_FLAG =handleException(ex)
        throw ex
    } finally {
      if(EXCEPTION_FLAG)close()
    }
  }

  override def zrangeByscore(key: String, min: Double, max: Double): Set[String] = {
    try {
      cluster.zrangeByScore(key,min,max).toSet
    } catch {
      case ex:Exception =>
        EXCEPTION_FLAG =handleException(ex)
        throw ex
    } finally {
      if(EXCEPTION_FLAG)close()
    }
  }


  override def zadd(key: String, score: Double, value: String): Long = {
    try {
      cluster.zadd(key,score,value)
    } catch {
      case ex:Exception =>
        EXCEPTION_FLAG =handleException(ex)
        throw ex
    } finally {
      if(EXCEPTION_FLAG)close()
    }
  }

  override def zrevrangeByScore(key: String, max: Double, min: Double, start: Int, limit: Int): Set[String] = {
    try {
      cluster.zrangeByScore(key,max,min,start,limit).toSet
    } catch {
      case ex:Exception =>
        EXCEPTION_FLAG =handleException(ex)
        throw ex
    } finally {
      if(EXCEPTION_FLAG)close()
    }
  }

  override def zrem(key: String, value: String): Long = {
    try {
      cluster.zrem(key,value)
    } catch {
      case ex:Exception =>
        EXCEPTION_FLAG =handleException(ex)
        throw ex
    } finally {
      if(EXCEPTION_FLAG)close()
    }
  }

  override def sMerbers(key: String): Set[String] ={
    try {
      cluster.smembers(key).toSet
    } catch {
      case ex:Exception =>
        EXCEPTION_FLAG =handleException(ex)
        throw ex
    } finally {
      if(EXCEPTION_FLAG)close()
    }
}

  override def sIsMember(key: String, member: String): Boolean = {
    try {
      cluster.sismember(key,member)
    } catch {
      case ex:Exception =>
        EXCEPTION_FLAG =handleException(ex)
        throw ex
    } finally {
      if(EXCEPTION_FLAG)close()
    }
  }

  override def sRem(key: String, member: String): Long = {
    try {
      cluster.srem(key,member)
    } catch {
      case ex:Exception =>
        EXCEPTION_FLAG =handleException(ex)
        throw ex
    } finally {
      if(EXCEPTION_FLAG)close()
    }
  }

  override def sAdd(key: String, member: String): Long ={
    try {
      cluster.sadd(key,member)
    } catch {
      case ex:Exception =>
        EXCEPTION_FLAG =handleException(ex)
        throw ex
    } finally {
      if(EXCEPTION_FLAG)close()
    }
}
//存在问题，待查资料
  override def sDel(key: String, member: String): Long = {
    try {
      cluster.srem(key,member)
    } catch {
      case ex:Exception =>
        EXCEPTION_FLAG =handleException(ex)
        throw ex
    } finally {
      if(EXCEPTION_FLAG)close()
    }
  }

  override def hGet(key: String, value: String): String = {
    try {
      cluster.hget(key,value)
    } catch {
      case ex:Exception =>
        EXCEPTION_FLAG =handleException(ex)
        throw ex
    } finally {
      if(EXCEPTION_FLAG)close()
    }
  }

  override def hGetAll(key: String): Map[String, String] = {
    try {
      cluster.hgetAll(key).toMap
    } catch {
      case ex:Exception =>
        EXCEPTION_FLAG =handleException(ex)
        throw ex
    } finally {
      if(EXCEPTION_FLAG)close()
    }
  }

  override def hGetValues(key: String): List[String] = {
    try {
      cluster.hvals(key).toList
    } catch {
      case ex:Exception =>
        EXCEPTION_FLAG =handleException(ex)
        throw ex
    } finally {
      if(EXCEPTION_FLAG)close()
    }
  }

  override def hSet(key: String, field: String, value: String): Long = {
    try {
      cluster.hset(key,field,value)
    } catch {
      case ex:Exception =>
        EXCEPTION_FLAG =handleException(ex)
        throw ex
    } finally {
      if(EXCEPTION_FLAG)close()
    }
  }

  override def hMSet(key: String, values: Map[String, String]): String = {
    try {
      cluster.hmset(key,values)
    } catch {
      case ex:Exception =>
        EXCEPTION_FLAG =handleException(ex)
        throw ex
    } finally {
      if(EXCEPTION_FLAG)close()
    }
  }

  override def hDel(key: String, field: String): Long = {
    try {
      cluster.hdel(key,field)
    } catch {
      case ex:Exception =>
        EXCEPTION_FLAG =handleException(ex)
        throw ex
    } finally {
      if(EXCEPTION_FLAG)close()
    }
  }

  override def close(): Unit = {
    try {
      cluster.close()
      ALIVE = false
      println(s"关掉了${redisName}连接：$ALIVE")
      LOG.info(s"关掉了${redisName}连接：$ALIVE")
    } catch {
      case ex:Exception =>ex.printStackTrace()
    }
}

  override def lpush(key: String, value: String): Long = {
      try {
        cluster.lpush(key,value)
    } catch {
      case ex:Exception =>
        EXCEPTION_FLAG =handleException(ex)
        throw ex
    } finally {
      if(EXCEPTION_FLAG)close()
    }
  }

  override def lrange(key: String, start: Long, end: Long): List[String] = {
    try {
      cluster.lrange(key,start,end).toList
    } catch {
      case ex:Exception =>
        EXCEPTION_FLAG =handleException(ex)
        throw ex
    } finally {
      if(EXCEPTION_FLAG)close()
    }
  }

  override def llen(key: String): Long = {
    try {
      cluster.llen(key)
    } catch {
      case ex:Exception =>
        EXCEPTION_FLAG =handleException(ex)
        throw ex
    } finally {
      if(EXCEPTION_FLAG)close()
    }
  }

  override def lrem(key: String, count: Long, value: String): Long = {
    try {
      cluster.lrem(key,count,value)
    } catch {
      case ex:Exception =>
        EXCEPTION_FLAG =handleException(ex)
        throw ex
    } finally {
      if(EXCEPTION_FLAG)close()
    }
  }

  override def exist(key: String): Boolean = {
    try {
      cluster.exists(key)
    } catch {
      case ex:Exception =>
        EXCEPTION_FLAG =handleException(ex)
        throw ex
    } finally {
      if(EXCEPTION_FLAG)close()
    }
  }

  override def scan(cuser: String, scanParams: ScanParams): ScanResult[String] = {
    try {
      cluster.scan(cuser,scanParams)
    } catch {
      case ex:Exception =>
        EXCEPTION_FLAG =handleException(ex)
        throw ex
    } finally {
      if(EXCEPTION_FLAG)close()
    }
  }

  override def hincrBy(key: String, value: String, incr: Int): Long = {
    try {
      cluster.hincrBy(key,value,incr)
    } catch {
      case ex:Exception =>
        EXCEPTION_FLAG =handleException(ex)
        throw ex
    } finally {
      if(EXCEPTION_FLAG)close()
    }
  }

  override def incrBy(key: String, incr: Int): Long = {
    try {
      cluster.incrBy(key,incr)
    } catch {
      case ex:Exception =>
        EXCEPTION_FLAG =handleException(ex)
        throw ex
    } finally {
      if(EXCEPTION_FLAG)close()
    }
  }


  override def hexists(key: String, field: String): Boolean = {
    try {
      cluster.hexists(key,field)
    } catch {
      case ex:Exception =>
        EXCEPTION_FLAG =handleException(ex)
        throw ex
    } finally {
      if(EXCEPTION_FLAG)close()
    }
  }


  override def zrangByScoreWithScores(key: String, min: Double, max: Double): Set[Tuple] = {
    try {
      cluster.zrangeByScoreWithScores(key,min,max).toSet
    } catch {
      case ex:Exception =>
        EXCEPTION_FLAG =handleException(ex)
        throw ex
    } finally {
      if(EXCEPTION_FLAG)close()
    }
  }

  override def zrevrangeByScoreWithScores(key: String, max: Double, min: Double): Set[Tuple] = {
    try {
      cluster.zrevrangeByScoreWithScores(key,max,min).toSet
    } catch {
      case ex:Exception =>
        EXCEPTION_FLAG =handleException(ex)
        throw ex
    } finally {
      if(EXCEPTION_FLAG)close()
    }
  }

  override def append(key: String, value: String): Long = {
    try {
      cluster.append(key,value)
    } catch {
      case ex:Exception =>
        EXCEPTION_FLAG =handleException(ex)
        throw ex
    } finally {
      if(EXCEPTION_FLAG)close()
    }
  }

  override def rpush(key: String, value: String): Long = {
    try {
      cluster.rpush(key,value)
    } catch {
      case ex:Exception =>
        EXCEPTION_FLAG =handleException(ex)
        throw ex
    } finally {
      if(EXCEPTION_FLAG)close()
    }
  }
}
