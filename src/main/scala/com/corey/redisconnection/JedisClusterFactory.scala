package com.corey.redisconnection

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{HostAndPort, JedisCluster}

import scala.collection.JavaConversions.setAsJavaSet

object JedisClusterFactory {

  private def parseToRedisNodes(bootstrapServers: String) = {
    bootstrapServers.split(",").toSet[String].map(_.split(":")).map(n =>
      new HostAndPort(n(0), n(1).toInt))
  }

  /**
    * 需要密码
    * 格式：host1:port1,host2:port2
    */
  def create(bootstrapServers: String, password: String): JedisCluster = {
    new JedisCluster(parseToRedisNodes(bootstrapServers), ConfigUtil.CONNECTION_TIMEOUT, ConfigUtil.SO_TIMEOUT,
      ConfigUtil.MAX_REDIRECTIONS, password, new GenericObjectPoolConfig)
  }

  /**
    * 不需要密码
    * 格式：host1:port1,host2:port2
    */
  def create(bootstrapServers: String): JedisCluster = {
    new JedisCluster(parseToRedisNodes(bootstrapServers))
  }
}
