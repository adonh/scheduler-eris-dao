# eris-dao [![Build Status](https://travis-ci.org/PagerDuty/eris-dao.svg?branch=master)](https://travis-ci.org/PagerDuty/eris-dao/builds)

This is an open source project!

## Description

Eris-dao contains basic Data Access Object (DAO) traits.

Key features:
 * MapperDAO provides basic CRUD methods using [Entity-mapper](https://github.com/PagerDuty/eris-mapper)

## Installation

This library is published to PagerDuty Bintray OSS Maven repository:
```scala
resolvers += "bintray-pagerduty-oss-maven" at "https://dl.bintray.com/pagerduty/oss-maven"
```

Adding the dependency to your SBT build file:
```scala
libraryDependencies += "com.pagerduty" %% "eris-dao" % "1.6.1"
```

## Contact

This library is primarily maintained by the Core Team at PagerDuty.

## Contributing

Contributions are welcome in the form of pull-requests based on the master branch. We ask that your changes are covered by unit tests.

Before opening a pull-request, remember to apply auto-formatting to your code. You trigger auto-formatting by running `sbt test` or `sbt ";compile;test:compile"`.

## Release

Follow these steps to release a new version:
 - Update version.sbt in your PR
 - Update CHANGELOG.md in your PR
 - When the PR is approved, merge it to master, and delete the branch
 - Travis will run all tests, publish artifacts to Bintray, and create a new version tag in Github

## Changelog

See [CHANGELOG.md](./CHANGELOG.md)
