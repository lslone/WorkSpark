package com.corey.main

import com.corey.bean.SourceBean
import com.corey.handler.{Engine, OutHandler}


object Main {
  def main(args: Array[String]): Unit = {
    new Engine[SourceBean].run(args, new OutHandler)
  }
}
