/**
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com/>
 */
package akka.typed
package internal

import java.util.concurrent.ThreadLocalRandom

import scala.annotation.tailrec
import scala.concurrent.duration.Deadline
import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag
import scala.util.control.Exception.Catcher
import scala.util.control.NonFatal

import akka.actor.DeadLetterSuppression
import akka.annotation.InternalApi
import akka.event.Logging
import akka.typed.ActorContext
import akka.typed.Behavior
import akka.typed.Behavior.DeferredBehavior
import akka.typed.ExtensibleBehavior
import akka.typed.PreRestart
import akka.typed.Signal
import akka.typed.SupervisorStrategy._
import akka.typed.scaladsl.Actor._
import akka.util.OptionVal

/**
 * INTERNAL API
 */
@InternalApi private[akka] object Restarter {
  def apply[T, Thr <: Throwable: ClassTag](initialBehavior: Behavior[T], strategy: SupervisorStrategy): Behavior[T] =
    strategy match {
      case Restart(-1, _, loggingEnabled) ⇒ new Restarter(initialBehavior, loggingEnabled)()
      case r: Restart ⇒
        new LimitedRestarter(initialBehavior, r, retries = 0, deadline = OptionVal.None)()
      case Resume(loggingEnabled) ⇒ new Resumer(initialBehavior, loggingEnabled)
      case b: Backoff ⇒
        val backoffRestarter =
          new BackoffRestarter(initialBehavior.asInstanceOf[Behavior[Any]], b, restartCount = 0, blackhole = false)()
        backoffRestarter.asInstanceOf[Behavior[T]]
    }
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] abstract class Supervisor[T, Thr <: Throwable: ClassTag] extends ExtensibleBehavior[T] {

  protected def loggingEnabled: Boolean

  /**
   * Current behavior
   */
  protected def behavior: Behavior[T]

  /**
   * Wrap next behavior in a concrete restarter again.
   */
  protected def wrap(nextBehavior: Behavior[T], afterException: Boolean): Supervisor[T, Thr]

  protected def handleException(ctx: ActorContext[T], startedBehavior: Behavior[T]): Catcher[Supervisor[T, Thr]]

  protected def restart(ctx: ActorContext[T], initialBehavior: Behavior[T], startedBehavior: Behavior[T]): Supervisor[T, Thr] = {
    try Behavior.interpretSignal(startedBehavior, ctx, PreRestart) catch { case NonFatal(_) ⇒ }
    // no need to canonicalize, it's done in the calling methods
    wrap(Behavior.validateAsInitial(Behavior.undefer(initialBehavior, ctx)), afterException = true)
  }

  protected def canonical(b: Behavior[T], ctx: ActorContext[T], afterException: Boolean): Behavior[T] =
    if (Behavior.isUnhandled(b)) Unhandled
    else if ((b eq Behavior.SameBehavior) || (b eq behavior)) Same
    else if (!Behavior.isAlive(b)) Stopped
    else {
      b match {
        case d: DeferredBehavior[T] ⇒ canonical(Behavior.undefer(d, ctx), ctx, afterException)
        case b                      ⇒ wrap(b, afterException)
      }
    }

  private def preStart(b: Behavior[T], ctx: ActorContext[T]): Behavior[T] =
    Behavior.undefer(b, ctx)

  override def management(ctx: ActorContext[T], signal: Signal): Behavior[T] = {
    val startedBehavior = preStart(behavior, ctx)
    try {
      val b = Behavior.interpretSignal(startedBehavior, ctx, signal)
      canonical(b, ctx, afterException = false)
    } catch handleException(ctx, startedBehavior)
  }

  override def message(ctx: ActorContext[T], msg: T): Behavior[T] = {
    val startedBehavior = preStart(behavior, ctx)
    try {
      val b = Behavior.interpretMessage(startedBehavior, ctx, msg)
      canonical(b, ctx, afterException = false)
    } catch handleException(ctx, startedBehavior)
  }

  protected def log(ctx: ActorContext[T], ex: Thr): Unit = {
    if (loggingEnabled)
      ctx.system.eventStream.publish(Logging.Error(ex, ctx.self.toString, behavior.getClass, ex.getMessage))
  }
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final class Resumer[T, Thr <: Throwable: ClassTag](
  override val behavior: Behavior[T], override val loggingEnabled: Boolean) extends Supervisor[T, Thr] {

  override def handleException(ctx: ActorContext[T], startedBehavior: Behavior[T]): Catcher[Supervisor[T, Thr]] = {
    case NonFatal(ex: Thr) ⇒
      log(ctx, ex)
      wrap(startedBehavior, afterException = true)
  }

  override protected def wrap(nextBehavior: Behavior[T], afterException: Boolean): Supervisor[T, Thr] =
    new Resumer[T, Thr](nextBehavior, loggingEnabled)

}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final class Restarter[T, Thr <: Throwable: ClassTag](
  initialBehavior: Behavior[T], override val loggingEnabled: Boolean)(
  override val behavior: Behavior[T] = initialBehavior) extends Supervisor[T, Thr] {

  override def handleException(ctx: ActorContext[T], startedBehavior: Behavior[T]): Catcher[Supervisor[T, Thr]] = {
    case NonFatal(ex: Thr) ⇒
      log(ctx, ex)
      restart(ctx, initialBehavior, startedBehavior)
  }

  override protected def wrap(nextBehavior: Behavior[T], afterException: Boolean): Supervisor[T, Thr] =
    new Restarter[T, Thr](initialBehavior, loggingEnabled)(nextBehavior)
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final class LimitedRestarter[T, Thr <: Throwable: ClassTag](
  initialBehavior: Behavior[T], strategy: Restart,
  retries: Int, deadline: OptionVal[Deadline])(
  override val behavior: Behavior[T] = initialBehavior) extends Supervisor[T, Thr] {

  override def loggingEnabled: Boolean = strategy.loggingEnabled

  private def deadlineHasTimeLeft: Boolean = deadline match {
    case OptionVal.None    ⇒ true
    case OptionVal.Some(d) ⇒ d.hasTimeLeft
  }

  override def handleException(ctx: ActorContext[T], startedBehavior: Behavior[T]): Catcher[Supervisor[T, Thr]] = {
    case NonFatal(ex: Thr) ⇒
      log(ctx, ex)
      if (deadlineHasTimeLeft && retries >= strategy.maxNrOfRetries)
        throw ex
      else
        restart(ctx, initialBehavior, startedBehavior)
  }

  override protected def wrap(nextBehavior: Behavior[T], afterException: Boolean): Supervisor[T, Thr] = {
    if (afterException) {
      val timeLeft = deadlineHasTimeLeft
      val newRetries = if (timeLeft) retries + 1 else 1
      val newDeadline = if (deadline.isDefined && timeLeft) deadline else OptionVal.Some(Deadline.now + strategy.withinTimeRange)
      new LimitedRestarter[T, Thr](initialBehavior, strategy, newRetries, newDeadline)(nextBehavior)
    } else
      new LimitedRestarter[T, Thr](initialBehavior, strategy, retries, deadline)(nextBehavior)
  }
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object BackoffRestarter {
  /**
   * Calculates an exponential back off delay.
   */
  def calculateDelay(
    restartCount: Int,
    minBackoff:   FiniteDuration,
    maxBackoff:   FiniteDuration,
    randomFactor: Double): FiniteDuration = {
    val rnd = 1.0 + ThreadLocalRandom.current().nextDouble() * randomFactor
    if (restartCount >= 30) // Duration overflow protection (> 100 years)
      maxBackoff
    else
      maxBackoff.min(minBackoff * math.pow(2, restartCount)) * rnd match {
        case f: FiniteDuration ⇒ f
        case _                 ⇒ maxBackoff
      }
  }

  case object ScheduledRestart
  final case class ResetRestartCount(current: Int) extends DeadLetterSuppression
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final class BackoffRestarter[T, Thr <: Throwable: ClassTag](
  initialBehavior: Behavior[Any], strategy: Backoff,
  restartCount: Int, blackhole: Boolean)(
  override val behavior: Behavior[Any] = initialBehavior) extends Supervisor[Any, Thr] {

  // FIXME using Any here because the scheduled messages can't be of type T.
  //       Something to consider is that timer messages should typically not be part of the
  //       ordinary public message protocol and therefore those should perhaps be signals.
  //       https://github.com/akka/akka/issues/16742

  import BackoffRestarter._

  override def loggingEnabled: Boolean = strategy.loggingEnabled

  override def management(ctx: ActorContext[Any], signal: Signal): Behavior[Any] = {
    if (blackhole) {
      ctx.system.eventStream.publish(Dropped(signal, ctx.self))
      Same
    } else
      super.management(ctx, signal)
  }

  override def message(ctx: ActorContext[Any], msg: Any): Behavior[Any] = {
    // intercept the scheduled messages and drop incoming messages if we are in backoff mode
    msg match {
      case ScheduledRestart ⇒
        // actual restart after scheduled backoff delay
        val restartedBehavior = Behavior.validateAsInitial(Behavior.undefer(initialBehavior, ctx))
        ctx.schedule(strategy.resetBackoffAfter, ctx.self, ResetRestartCount(restartCount))
        new BackoffRestarter[T, Thr](initialBehavior, strategy, restartCount, blackhole = false)(restartedBehavior)
      case ResetRestartCount(current) ⇒
        if (current == restartCount)
          new BackoffRestarter[T, Thr](initialBehavior, strategy, restartCount = 0, blackhole)(behavior)
        else
          Same
      case _ ⇒
        if (blackhole) {
          ctx.system.eventStream.publish(Dropped(msg, ctx.self))
          Same
        } else
          super.message(ctx, msg)
    }
  }

  override def handleException(ctx: ActorContext[Any], startedBehavior: Behavior[Any]): Catcher[Supervisor[Any, Thr]] = {
    case NonFatal(ex: Thr) ⇒
      log(ctx, ex)
      // actual restart happens after the scheduled backoff delay
      try Behavior.interpretSignal(behavior, ctx, PreRestart) catch { case NonFatal(_) ⇒ }
      val restartDelay = calculateDelay(restartCount, strategy.minBackoff, strategy.maxBackoff, strategy.randomFactor)
      ctx.schedule(restartDelay, ctx.self, ScheduledRestart)
      new BackoffRestarter[T, Thr](initialBehavior, strategy, restartCount + 1, blackhole = true)(startedBehavior)
  }

  override protected def wrap(nextBehavior: Behavior[Any], afterException: Boolean): Supervisor[Any, Thr] = {
    if (afterException)
      throw new IllegalStateException("wrap not expected afterException in BackoffRestarter")
    else
      new BackoffRestarter[T, Thr](initialBehavior, strategy, restartCount, blackhole)(nextBehavior)
  }
}

