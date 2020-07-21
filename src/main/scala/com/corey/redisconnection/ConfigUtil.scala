package com.corey.redisconnection

object ConfigUtil {
  final val NUM_5: Int = Integer.parseInt("5")
  final val NUM_2000: Int = Integer.parseInt("2000")
  final var REDIS_SERVER: String = ""
  final var CONNECTION_TIMEOUT: Int = NUM_2000
  final var SO_TIMEOUT: Int = NUM_2000
  final var MAX_REDIRECTIONS: Int = NUM_5

  def setProperty(redisServer: String, connectionTimeOut: Int = NUM_2000,
                  soTimeOut: Int = NUM_2000, maxRedirections: Int = NUM_5): Unit = {
    REDIS_SERVER = redisServer
    CONNECTION_TIMEOUT = connectionTimeOut
    SO_TIMEOUT = soTimeOut
    MAX_REDIRECTIONS = maxRedirections
  }

}
