/**
 * Copyright (C) 2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.typed
package internal

import scala.concurrent.duration.FiniteDuration

import akka.actor.Cancellable
import akka.annotation.ApiMayChange
import akka.annotation.DoNotInherit
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.typed.ActorRef
import akka.typed.ActorRef.ActorRefOps
import akka.typed.scaladsl
import akka.typed.scaladsl.ActorContext

/**
 * INTERNAL API
 */
@InternalApi private[akka] object TimerSchedulerImpl {
  final case class Timer[T](key: Any, msg: T, repeat: Boolean, generation: Int,
    adapter: ActorRef[TimerMsg], task: Cancellable)
  final case class TimerMsg(key: Any, generation: Int)
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class TimerSchedulerImpl[T](ctx: ActorContext[T]) extends scaladsl.TimerScheduler[T] {
  import TimerSchedulerImpl._

  // FIXME javadsl

  // FIXME change to a class specific logger, see issue #21219
  private val log = ctx.system.log
  private var timers: Map[Any, Timer[T]] = Map.empty

  override def startTimer(key: Any, msg: T, timeout: FiniteDuration, repeat: Boolean): Unit = {
    val nextGen = timers.get(key) match {
      case None ⇒ 1
      case Some(t) ⇒
        cancelTimer(t)
        t.generation + 1
    }

    val adapter: ActorRef[TimerMsg] = ctx.spawnAdapter { timerMsg ⇒
      timers.get(timerMsg.key) match {
        case None ⇒
          log.debug("Received timer [{}], but it has been removed", key)
          // FIXME the message should be ignored, and adapter removed
          throw new IllegalStateException(s"# No timer with key $key")
        case Some(t) ⇒
          // FIXME the generation is not needed, since we have a separate adapter ActorRef for each
          if (timerMsg.generation == t.generation) {
            log.debug("Received timer [{}]", key)
            t.msg
          } else {
            log.debug(
              "Received timer [{}], but from old generation [{}], expected generation [{}]",
              key, timerMsg.generation, t.generation)
            // FIXME the message should be ignored, and adapter removed
            cancelTimer(t)
            throw new IllegalStateException(s"# Old generation timer with key $key")
          }
      }
    }
    // FIXME implement repeated = false, and test that
    val task = ctx.system.scheduler.schedule(timeout, timeout) {
      adapter ! TimerMsg(key, nextGen)
    }(ExecutionContexts.sameThreadExecutionContext)

    val nextTimer = Timer(key, msg, repeat, nextGen, adapter, task)
    log.debug("Start timer [{}] with generation [{}] via adapter {}", key, nextGen, adapter)
    timers = timers.updated(key, nextTimer)
  }

  override def isTimerActive(key: Any): Boolean =
    timers.contains(key)

  override def cancel(key: Any): Unit = {
    timers.get(key) match {
      case None ⇒
      case Some(t) ⇒
        cancelTimer(t)
    }
  }

  private def cancelTimer(timer: Timer[T]): Unit = {
    log.debug("Cancel timer [{}] with generation [{}]", timer.key, timer.generation)
    timer.task.cancel()
    stopAdapter(timer.key, timer.adapter)
    timers -= timer.key
  }

  private def stopAdapter(key: Any, adapter: ActorRef[TimerMsg]): Unit = {
    if (!ctx.stop(adapter))
      log.warning("Couldn't remove timer [{}] adapter [{}]", key, adapter)
  }

  override def cancelAll(): Unit = {
    log.debug("Cancel all timers")
    timers.valuesIterator.foreach { timer ⇒
      timer.task.cancel()
      stopAdapter(timer.key, timer.adapter)
    }
    timers = Map.empty
  }

}
