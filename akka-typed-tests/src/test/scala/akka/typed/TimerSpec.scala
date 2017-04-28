/**
 * Copyright (C) 2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.typed

import scala.concurrent.duration._
import scala.util.control.NoStackTrace

import akka.typed.scaladsl.Actor._
import akka.typed.scaladsl.AskPattern._
import akka.typed.scaladsl.TimerScheduler
import akka.typed.testkit.TestKitSettings
import akka.typed.testkit.scaladsl._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class TimerSpec extends TypedSpec("""
  akka.loglevel = DEBUG
  """) {

  sealed trait Command
  case class Tick(n: Int) extends Command
  case object Bump extends Command
  case object End extends Command
  case class Throw(e: Throwable) extends Command
  case object Cancel extends Command

  sealed trait Event
  case class Tock(n: Int) extends Event

  class Exc extends RuntimeException("simulated exc") with NoStackTrace

  trait RealTests extends StartSupport {
    implicit def system: ActorSystem[TypedSpec.Command]
    implicit val testSettings = TestKitSettings(system)

    val interval = 1.second
    val dilatedInterval = interval.dilated

    def target(monitor: ActorRef[Event], timer: TimerScheduler[Command], bumpCount: Int): Behavior[Command] =
      Immutable { (ctx, cmd) ⇒
        cmd match {
          case Tick(n) ⇒
            monitor ! Tock(n)
            Same
          case Bump ⇒
            val nextCount = bumpCount + 1
            timer.startTimer("T", Tick(nextCount), interval, repeat = true)
            target(monitor, timer, nextCount)
          case End ⇒
            Stopped
          case Cancel ⇒
            timer.cancel("T")
            Same
          case Throw(e) ⇒
            throw e
        }
      }

    def `must schedule repeated ticks`(): Unit = {
      val probe = TestProbe[Event]("evt")
      val behv = timerScheduler[Command] { timer ⇒
        timer.startTimer("T", Tick(1), interval, repeat = true)
        target(probe.ref, timer, 1)
      }

      val ref = start(behv)
      probe.within((interval * 4) - 100.millis) {
        probe.expectMsg(Tock(1))
        probe.expectMsg(Tock(1))
        probe.expectMsg(Tock(1))
      }
      ref ! End
    }

    def `must replace timer`(): Unit = {
      val probe = TestProbe[Event]("evt")
      val behv = timerScheduler[Command] { timer ⇒
        timer.startTimer("T", Tick(1), interval, repeat = true)
        target(probe.ref, timer, 1)
      }

      val ref = start(behv)
      probe.expectMsg(Tock(1))
      ref ! Bump
      probe.expectMsg(Tock(2))
      ref ! End
    }

    def `must cancel timer`(): Unit = {
      val probe = TestProbe[Event]("evt")
      val behv = timerScheduler[Command] { timer ⇒
        timer.startTimer("T", Tick(1), interval, repeat = true)
        target(probe.ref, timer, 1)
      }

      val ref = start(behv)
      probe.expectMsg(Tock(1))
      ref ! Cancel
      probe.expectNoMsg(dilatedInterval + 100.millis)
      ref ! End
    }

    def `must discard timers from old incarnation after restart, alt 1`(): Unit = {
      val probe = TestProbe[Event]("evt")
      val behv = Restarter[Exception]().wrap(timerScheduler[Command] { timer ⇒
        timer.startTimer("T", Tick(1), interval, repeat = true)
        target(probe.ref, timer, 1)
      })

      val ref = start(behv)
      probe.expectMsg(Tock(1))
      // change state so that we see that the restart starts over again
      ref ! Bump
      probe.expectMsg(Tock(2))

      ref ! Throw(new Exc)
      probe.expectMsg(Tock(1))
      ref ! End
    }

    def `must discard timers from old incarnation after restart, alt 2`(): Unit = {
      // FIXME something wrong here: IllegalArgumentException: deferred [Deferred(Restarter.scala:35-50)] should not be passed to interpreter
      pending
      val probe = TestProbe[Event]("evt")
      val behv = timerScheduler[Command] { timer ⇒
        timer.startTimer("T", Tick(1), interval, repeat = true)
        Restarter[Exception]().wrap(target(probe.ref, timer, 1))
      }

      val ref = start(behv)
      probe.expectMsg(Tock(1))
      // change state so that we see that the restart starts over again
      ref ! Bump
      probe.expectMsg(Tock(2))

      ref ! Throw(new Exc)
      probe.expectMsg(Tock(1))
      ref ! End
    }

    // TODO illustrate how to use with MutableBehavior, but I think it's no different

  }

  object `A Restarter (real, native)` extends RealTests with NativeSystem
  //  object `A Restarter (real, adapted)` extends RealTests with AdaptedSystem

}
