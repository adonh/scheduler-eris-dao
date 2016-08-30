package com.pagerduty.eris.dao

import scala.concurrent.duration._

class Stopwatch {
  val startedAtNanos = System.nanoTime()
  def elapsed(): Duration = {
    (System.nanoTime() - startedAtNanos).nanos
  }
}

object Stopwatch {
  def start(): Stopwatch = new Stopwatch
}
