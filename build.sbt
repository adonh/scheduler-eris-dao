organization := "com.pagerduty"

name := "eris-dao"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
  "com.pagerduty" %% "eris-core" % "0.2" % "compile->compile;test->test",
  "com.pagerduty" %% "eris-mapper" % "0.2")

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.4" % Test,
  "org.scalamock" %% "scalamock-scalatest-support" % "3.2" % Test,
  "org.scalacheck" %% "scalacheck" % "1.12.3" % Test)
