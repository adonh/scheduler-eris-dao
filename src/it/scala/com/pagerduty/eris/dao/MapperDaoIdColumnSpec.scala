/*
 * Copyright (c) 2015, PagerDuty
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this list of conditions
 * and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice, this list of
 * conditions and the following disclaimer in the documentation and/or other materials provided with
 * the distribution.
 *
 * 3. Neither the name of the copyright holder nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
 * WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.pagerduty.eris.dao

import com.netflix.astyanax.{ Keyspace, Cluster }
import com.pagerduty.eris.serializers._
import com.pagerduty.eris.schema.SchemaLoader
import com.pagerduty.eris.{ TimeUuid, TestClusterCtx }
import com.pagerduty.mapper.annotations._
import org.scalatest.{ Outcome, Matchers }
import org.scalatest.fixture.FreeSpec
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContextExecutor, Await, Future}
import scala.util.Random

object MapperDaoIdColumnSpec {
  @Entity case class TestEntityIdNotColumn(
      @Id id: TimeUuid,
      @Column(name = "data") data: Double
  ) {
    def this() = this(null, Double.NaN)
    def toIdAlsoColumn = TestEntityIdAlsoColumn(id, data)
  }
  class TestDaoIdNotColumn(protected val cluster: Cluster, protected val keyspace: Keyspace)
      extends MapperDao[TimeUuid, TestEntityIdNotColumn] {
    protected def executor: ExecutionContextExecutor = scala.concurrent.ExecutionContext.global
    val entityClass = classOf[TestEntityIdNotColumn]
    val mainFamily = entityColumnFamily("testIdColumnDaoMainCf")()

    def find(id: TimeUuid) = mapperFind(id)
    def getId(entity: TestEntityIdNotColumn) = entityMapper.getId(entity).get
    def persist(entity: TestEntityIdNotColumn) = mapperPersist(getId(entity), entity)
  }

  @Entity case class TestEntityIdAlsoColumn(
      @Id @Column(name = "idCol") id: TimeUuid,
      @Column(name = "data") data: Double
  ) {
    def this() = this(null, Double.NaN)
    def toIdNotColumn = TestEntityIdNotColumn(id, data)
  }
  class TestDaoIdAlsoColumn(protected val cluster: Cluster, protected val keyspace: Keyspace)
      extends MapperDao[TimeUuid, TestEntityIdAlsoColumn] {
    protected def executor: ExecutionContextExecutor = scala.concurrent.ExecutionContext.global
    val entityClass = classOf[TestEntityIdAlsoColumn]
    val mainFamily = entityColumnFamily("testIdColumnDaoMainCf")()

    def find(id: TimeUuid) = mapperFind(id)
    def getId(entity: TestEntityIdAlsoColumn) = entityMapper.getId(entity).get
    def persist(entity: TestEntityIdAlsoColumn) = mapperPersist(getId(entity), entity)
  }

  case class TestDaos(idNotCol: TestDaoIdNotColumn, idAlsoCol: TestDaoIdAlsoColumn)
}

class MapperDaoIdColumnSpec extends FreeSpec with Matchers {
  import MapperDaoIdColumnSpec._

  type FixtureParam = TestDaos

  override def withFixture(test: OneArgTest): Outcome = {
    val cluster = TestClusterCtx.cluster
    val keyspace = cluster.getKeyspace("MapperDaoIdColumnSpec" + Thread.currentThread.getId)
    val daos = TestDaos(
      new TestDaoIdNotColumn(cluster, keyspace),
      new TestDaoIdAlsoColumn(cluster, keyspace)
    )
    val schemaLoader = new SchemaLoader(cluster, daos.idAlsoCol.columnFamilyDefs)

    try {
      schemaLoader.loadSchema()
      withFixture(test.toNoArgTest(daos))
    } finally {
      schemaLoader.dropSchema()
    }
  }

  def wait[T](future: Future[T]): T = Await.result(future, Duration.Inf)

  "MapperDao should allow changing whether the ID field is also stored as a column or not" - {

    "be backward- and forward-compatible data-wise when the change is deployed" - {

      "be able to read idNotCol data with idAlsoCol DAO" in { daos =>
        val entity = TestEntityIdNotColumn(TimeUuid(), math.random)

        wait(daos.idNotCol.persist(entity))
        wait(daos.idAlsoCol.find(entity.id)) shouldBe Some(entity.toIdAlsoColumn)
      }

      "be able to read idAlsoCol data with idNotCol DAO" in { daos =>
        val entity = TestEntityIdAlsoColumn(TimeUuid(), math.random)

        wait(daos.idAlsoCol.persist(entity))
        wait(daos.idNotCol.find(entity.id)) shouldBe Some(entity.toIdNotColumn)
      }
    }
  }
}
