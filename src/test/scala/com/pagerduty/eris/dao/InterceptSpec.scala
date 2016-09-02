package com.pagerduty.eris.dao

import com.pagerduty.metrics.{ NullMetrics, Stopwatch }
import org.scalatest.{ FreeSpec, Matchers }
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._

class InterceptSpec extends FreeSpec with Matchers {
  import scala.concurrent.ExecutionContext.Implicits.global
  def stopwatch = Stopwatch.start()

  "Interceptor.decorate() should" - {
    val t = QueryType.Read

    "when future completed before timeout" - {
      "return original result" in {
        val original = Future { Thread.sleep(100); "result" }
        val intercepted = Interceptor.decorate("cf", "q", t, NullMetrics)(stopwatch, original)
        Await.result(intercepted, 1.second) shouldBe "result"
      }
      "propagate original failure" in {
        class TestException extends Exception
        val original = Future { Thread.sleep(100); throw new TestException }
        val intercepted = Interceptor.decorate("cf", "q", t, NullMetrics)(stopwatch, original)
        intercept[TestException] { Await.result(intercepted, 1.second) }
      }
    }
  }
}
