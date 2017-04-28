/**
 * Copyright (C) 2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.typed.scaladsl

import scala.concurrent.duration.FiniteDuration

trait TimerScheduler[T] {

  // FIXME docs

  def startTimer(key: Any, msg: T, timeout: FiniteDuration, repeat: Boolean): Unit

  def isTimerActive(key: Any): Boolean

  def cancel(key: Any): Unit

  def cancelAll(): Unit

}
