package com.corey.redisconnection

import redis.clients.jedis.{ScanParams, ScanResult, Tuple}

trait RedisOperations {
  var EXCEPTION_FLAG = false
  var ALIVE = true

  def expire(key: String, expireSeconds: Int): Long

  def ttl(key: String): Long

  def get(key: String): String

  def del(key: String): Long

  def set(key: String, value: String): String

  def set(key: String, value: String, nxxx: String, expx: String, time: Long): String

  def zaddBatch(key: String, values: Iterable[(String, Double)]): Long

  def zremRangeByScore(key: String, minScore: Double, maxScore: Double): Long

  def zrangeByscore(key: String, min: Double, max: Double): Set[String]

  def zadd(key: String, score: Double, value: String): Long

  def zrevrangeByScore(key: String, max: Double, min: Double, start: Int, limit: Int): Set[String]

  def zrem(key: String, value: String): Long

  def sMerbers(key: String): Set[String]

  def sIsMember(key: String, member: String): Boolean

  def sRem(key: String, member: String): Long

  def sAdd(key: String, member: String): Long

  def sDel(key: String, member: String): Long


  def hGet(key: String, value: String): String

  def hGetAll(key: String): Map[String, String]

  def hGetValues(key: String): List[String]

  def hSet(key: String, field: String, value: String): Long

  def hMSet(key: String, values: Map[String, String]): String

  def hDel(key: String, field: String): Long

  def close(): Unit

  def lpush(key: String, value: String): Long

  def lrange(key: String, start: Long, end: Long): List[String]

  def llen(key: String): Long

  def lrem(key: String, count: Long, value: String): Long

  def exist(key: String): Boolean

  def scan(cuser: String, scanParams: ScanParams): ScanResult[String]

  def hincrBy(key: String, value: String, incr: Int): Long

  def incrBy(key: String, incr: Int): Long

  def hexists(key: String, field: String): Boolean

  def zrangByScoreWithScores(key: String, min: Double, max: Double): Set[Tuple]

  def zrevrangeByScoreWithScores(key: String, max: Double, min: Double): Set[Tuple]

  def append(key: String, value: String): Long

  def rpush(key: String, value: String): Long
}
