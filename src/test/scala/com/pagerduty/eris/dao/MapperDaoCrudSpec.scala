package com.pagerduty.eris.dao

import com.netflix.astyanax.{Keyspace, Cluster}
import com.pagerduty.eris.serializers._
import com.pagerduty.eris.schema.SchemaLoader
import com.pagerduty.eris.{TimeUuid, TestClusterCtx}
import org.scalatest.{Outcome, Matchers}
import org.scalatest.fixture.FreeSpec
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Random


class TestDao(protected val cluster: Cluster, protected val keyspace: Keyspace)
  extends MapperDao[TimeUuid, test.TestEntity]
{
  val entityClass = classOf[test.TestEntity]
  val mainFamily = entityColumnFamily("testDaoMainCf")()

  def find(id: TimeUuid) = mapperFind(id)
  def find(ids: Iterable[TimeUuid], batchSize: Int = 100) = mapperFind(ids, batchSize)
  def resolve(ids: Seq[TimeUuid], batchSize: Int = 100) = mapperResolve(ids, batchSize)
  def persist(id: TimeUuid, entity: test.TestEntity) = mapperPersist(id, entity)
  def remove(id: TimeUuid) = mapperRemove(id)
}


class MapperDaoCrudSpec extends FreeSpec with Matchers {
  type FixtureParam = TestDao

  override def withFixture(test: OneArgTest): Outcome = {
    val cluster = TestClusterCtx.cluster
    val keyspace = cluster.getKeyspace("MapperDaoCrudSpec" + Thread.currentThread.getId)
    val dao = new TestDao(cluster, keyspace)
    val schemaLoader = new SchemaLoader(cluster, dao.columnFamilyDefs)

    try {
      schemaLoader.loadSchema()
      withFixture(test.toNoArgTest(dao))
    }
    finally {
      schemaLoader.dropSchema()
    }
  }

  def wait[T](future: Future[T]): T = Await.result(future, Duration.Inf)


  "When doing CRUD MapperDao should" - {
    "persist, find, and remove correctly" in { dao =>
      val id = TimeUuid()
      val entity = test.TestEntity("a", 10)

      wait(dao.find(id)) shouldBe None
      wait(dao.persist(id, entity))
      wait(dao.find(id)) shouldBe Some(entity)
      wait(dao.remove(id))
      wait(dao.find(id)) shouldBe None
    }

    "find batch correctly" in { dao =>
      val data = for (i <- 0 until 10) yield (TimeUuid(), new test.TestEntity(i.toString(), i))
      val entries = data.toMap
      val partial = entries.take(5)
      for ((id, entity) <- entries) wait(dao.persist(id, entity))

      wait(dao.find(partial.keySet)) shouldBe partial
      wait(dao.find(entries.keySet, 2)) shouldBe entries
    }

    "preserve order with mapperResolve()" in { dao =>
      val data = for (i <- 0 until 10) yield (TimeUuid(), new test.TestEntity(i.toString(), i))
      for ((id, entity) <- data) wait(dao.persist(id, entity))
      val shuffled = new Random().shuffle(data)
      val ids = shuffled.map(_._1)
      val entries = shuffled.map(_._2)

      wait(dao.resolve(ids, 2)) shouldBe entries
    }

    "persist and load null fields correctly" in { dao =>
      val id = TimeUuid()
      val entity = test.TestEntity(null, 10)

      wait(dao.find(id)) shouldBe None
      wait(dao.persist(id, entity))
      wait(dao.find(id)) shouldBe Some(entity.copy(field0 = "default0"))
    }

    "persist and load fields serialized as empty byte buffer correctly" in { dao =>
      val id = TimeUuid()
      val entity = test.TestEntity("", 10)

      wait(dao.find(id)) shouldBe None
      wait(dao.persist(id, entity))
      wait(dao.find(id)) shouldBe Some(entity)
    }
  }
}
