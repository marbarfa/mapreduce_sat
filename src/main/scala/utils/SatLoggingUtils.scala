package main.scala.utils

import org.apache.log4j.Logger

/**
 * Created by marbarfa on 3/29/14.
 */
trait SatLoggingUtils {

  var log = Logger.getLogger(s"[3SATMR][${this.getClass.getSimpleName}]")

}
