package com.corey.redisconnection

import redis.clients.jedis.{ScanParams, ScanResult, Tuple}

object RedisClusterFactory extends Serializable {
  private lazy val proxy = RedisClusterProxy()

  def init(redisServer: String, connectionTimeout: Int = ConfigUtil.NUM_2000,
           soTimeout: Int = ConfigUtil.NUM_2000, maxRedirections: Int = ConfigUtil.NUM_5): Unit = {
    ConfigUtil.setProperty(redisServer, connectionTimeout, soTimeout, maxRedirections)
  }

  def closeAll(): Unit = {
    proxy.closeAll()
  }

  def set(key: String, value: String, redisName: String): String = {
    proxy.getService(redisName) set(key, value)

  }

  def get(key: String, redisName: String): String = {
    proxy.getService(redisName).get(key)
  }

  def expire(key: String, expireSeconds: Int, redisName: String): Long = {
    proxy.getService(redisName).expire(key, expireSeconds)
  }

  def hGetAll(key: String, redisName: String): Map[String, String] = {
    proxy.getService(redisName).hGetAll(key)
  }

  def lrem(key: String, count: Long, value: String, redisName: String): Long = {
    proxy.getService(redisName).lrem(key, count, value)
  }

  def sIsMember(key: String, member: String, redisName: String): Boolean = {
    proxy.getService(redisName).sIsMember(key, member)
  }

  def exist(key: String, redisName: String): Boolean = {
    proxy.getService(redisName).exist(key)
  }

  def set(key: String, value: String, nxxx: String, expx: String, time: Long, redisName: String): String = {
    proxy.getService(redisName).set(key, value, nxxx, expx, time)
  }

  def hGetValues(key: String, redisName: String): List[String] = {
    proxy.getService(redisName).hGetValues(key)
  }

  def hGet(key: String, filed: String, redisName: String): String = {
    proxy.getService(redisName).hGet(key, filed)
  }

  def hSet(key: String, filed: String, value: String, redisName: String): Long = {
    proxy.getService(redisName).hSet(key, filed, value)
  }

  def zrangeByScore(key: String, min: Double, max: Double, redisName: String): Set[String] = {
    proxy.getService(redisName).zrangeByscore(key, min, max)
  }

  def sRem(key: String, member: String, redisNeme: String): Long = {
    proxy.getService(redisNeme).sRem(key, member)
  }

  def hMSet(key: String, values: Map[String, String], redisName: String): String = {
    proxy.getService(redisName).hMSet(key, values)
  }

  def zaddBacth(key: String, values: Iterable[(String, Double)], redisName: String): Long = {
    proxy.getService(redisName).zaddBatch(key, values)
  }

  def sDel(key: String, member: String, redisName: String): Long = {
    proxy.getService(redisName).sDel(key, member)
  }

  def sAdd(key: String, member: String, redisName: String): Long = {
    proxy.getService(redisName).sAdd(key, member)
  }

  def incrBy(key: String, incr: Int, redisName: String): Long = {
    proxy.getService(redisName).incrBy(key, incr)
  }

  def scan(cusor: String, scanParams: ScanParams, redisName: String): ScanResult[String] = {
    proxy.getService(redisName).scan(cusor, scanParams)
  }

  def hincrBy(key: String, value: String, incr: Int, redisName: String): Long = {
    proxy.getService(redisName).hincrBy(key, value, incr)
  }

  def zremRangeByScore(key: String, minScore: Double, maxScore: Double, redisName: String): Long = {
    proxy.getService(redisName).zremRangeByScore(key, minScore, maxScore)
  }

  def llen(key: String, redisName: String): Long = {
    proxy.getService(redisName).llen(key)
  }

  def close(redisName: String): Unit = {
    proxy.getService(redisName).close()
  }

  def sMembers(key: String, redisName: String): Set[String] = {
    proxy.getService(redisName).sMerbers(key)
  }

  def zrangeByScoreWithScores(key: String, min: Double, max: Double, redisName: String): Set[Tuple] = {
    proxy.getService(redisName).zrevrangeByScoreWithScores(key, min, max)
  }

  def del(key: String, redisName: String) = {
    proxy.getService(redisName).del(key)
  }

  def zrem(key: String, value: String, redisName: String): Long = {
    proxy.getService(redisName).zrem(key, value)
  }

  def zrevrangeBycore(key: String, max: Double, min: Double, start: Int, limit: Int, redisName: String): Set[String] = {
    proxy.getService(redisName).zrevrangeByScore(key, max, min, start, limit)
  }

  def lrange(key: String, start: Long, end: Long, redisName: String): List[String] = {
    proxy.getService(redisName).lrange(key, start, end)
  }

  def zrevrangeByScoreWithScores(key: String, max: Double, min: Double, redisName: String): Set[Tuple] = {
    proxy.getService(redisName).zrevrangeByScoreWithScores(key, max, min)
  }

  def hDel(key: String, field: String, redisName: String): Long = {
    proxy.getService(redisName).hDel(key, field)
  }

  def ttl(key: String, redisName: String): Long = {
    proxy.getService(redisName).ttl(key)
  }

  def hexists(key: String, field: String, redisName: String): Boolean = {
    proxy.getService(redisName).hexists(key, field)
  }

  def zadd(key: String, score: Double, value: String, redisName: String): Long = {
    proxy.getService(redisName).zadd(key, score, value)
  }

  def lpush(key: String, value: String, redisName: String): Long = {
    proxy.getService(redisName).lpush(key, value)
  }

  def rpush(key: String, value: String, redisName: String): Long = {
    proxy.getService(redisName).rpush(key, value)
  }

  def append(key: String, value: String, redisName: String): Long = {
    proxy.getService(redisName).append(key, value)
  }
}
