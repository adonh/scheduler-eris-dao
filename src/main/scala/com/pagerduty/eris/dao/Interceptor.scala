package com.pagerduty.eris.dao

import com.pagerduty.metrics.Metrics
import scala.concurrent.Future
import scala.util.{ Failure, Success }

object QueryType extends Enumeration {
  val Read, Write = Value
}

object Interceptor {
  private val resultTag = "result"
  private val exceptionTag = "exception"

  /**
   * ColumnFamily-centric stats output with tags:
   * {{{
   * PdStats.addMetric("cassandra_msec", value,
   *   "query" -> "findById", "cf" -> cfName, "rw" -> "read")
   * PdStats.addMetric("cassandra_msec", value,
   *   "query" -> "batchFetch", "cf" -> cfName, "rw" -> "read")
   * }}}
   */
  def decorate[T](
    columnFamilyName: String,
    queryName: String,
    queryType: QueryType.Value,
    metrics: Metrics
  )(stopwatch: Stopwatch, future: Future[T]): Future[T] = {
    import scala.concurrent.ExecutionContext.Implicits.global

    def snakify(name: String) = name.replaceAll("([A-Z]+)([A-Z][a-z])", "$1_$2").replaceAll("([a-z\\d])([A-Z])", "$1_$2").toLowerCase

    future.onComplete { res =>
      val queryTags = Seq(
        "cf" -> snakify(columnFamilyName),
        "query" -> queryName,
        "rw" -> queryType.toString.toLowerCase
      )
      val resultTags = res match {
        case Success(_) =>
          Seq(resultTag -> "success")

        case Failure(e) =>
          Seq(resultTag -> "failure", exceptionTag -> snakify(e.getClass.getSimpleName))
      }
      val durationMs = stopwatch.elapsed().toMillis.toInt
      metrics.histogram("cassandra_msec", durationMs, queryTags ++ resultTags: _*)
    }
    future
  }
}
