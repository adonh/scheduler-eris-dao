package com.pagerduty.eris.dao

import com.netflix.astyanax.Serializer
import com.pagerduty.eris._
import com.pagerduty.eris.mapper.EntityMapper
import com.pagerduty.eris.serializers._
import FutureConversions._
import scala.collection.JavaConversions._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import com.pagerduty.eris.serializers.ValidatorClass


/**
 * MapperDao provides basic CRUD method for a target entity class.
 *
 * Example:
 * {{{
 * class MyDao(val cluster: Cluster) extends MapperDao[MyId, MyEntity] {
 *   val keyspace = cluster.getKeyspace("myKeyspace")
 *   val entityClass = classOf[MyEntity]
 *   val mainFamily = entityColumnFamily("myCf")()
 *
 *   def find(id: MyId) = mapperFind(id)
 *   def find(ids: Iterable[MyId], batchSize: Option[Int]) = mapperFind(ids, batchSize)
 *   def persist(id: MyId, entity: MyEntity) = mapperPersist(id, entity)
 *   def remove(id: MyId) = mapperRemove(id)
 * }
 * }}}
 */
trait MapperDao[Id, Entity] extends Dao {

  // Abstract members.
  /**
   * The target entity class
   * @return entity class
   */
  protected def entityClass: Class[Entity]

  /**
   * Main column family.
   */
  protected val mainFamily: ColumnFamilyModel[Id, String, Array[Byte]]

  // Defined members.
  /**
   * Entity mapper. Can be overridden to specify custom serializers.
   */
  protected lazy val entityMapper: EntityMapper[Id, Entity] = {
    new EntityMapper(entityClass, CommonSerializers)
  }

  /**
   * Shorthand method for defining the main column family. Key serializer
   * will be based on the Dao#Id type, and colName serializer will be StringSerializer.
   * By default, ColValue validator will be set to ValidatorClass[String], event though the
   * declared column value type is Array[Byte].
   */
  protected def entityColumnFamily
    (name: String, columnFamilySettings: ColumnFamilySettings = new ColumnFamilySettings)
    (columns: ColumnModel*)
    (implicit rowKeySerializer: Serializer[Id])
  : ColumnFamilyModel[Id, String, Array[Byte]] = {
    val defaultValueValidatorClass = columnFamilySettings
      .colValueValidatorOverride.getOrElse(ValidatorClass[String])

    val reflectionCols = entityMapper.columns
      .filterNot(_.validationClass == defaultValueValidatorClass)

    val colsByName =
      reflectionCols.map(col => col.name -> col).toMap ++
      columns.map(col => col.name -> col).toMap // Override with user specified values.

    columnFamily[Id, String, Array[Byte]](
      name,
      columnFamilySettings.copy(colValueValidatorOverride = Some(defaultValueValidatorClass)),
      colsByName.values.toSet)
  }

  /**
   * Find entity with a given id.
   *
   * @param id entity id
   * @return Some(entity) if exists, None otherwise.
   */
  protected def mapperFind(id: Id): Future[Option[Entity]] = {
    val query = keyspace.prepareQuery(mainFamily.columnFamily).getKey(id)
    query.executeAsync().map { res =>
      entityMapper.read(id, res.getResult)
    }
  }

  /**
   * Find a collection of entities with given ids, querying with maximum of `batchSize` number of
   * ids at a time.
   *
   * @param ids collection of ids
   * @param batchSize maximum number of ids to query at a time, unlimited when None
   * @return a map of ids to entities
   */
  protected def mapperFind(ids: Iterable[Id], batchSize: Option[Int]): Future[Map[Id, Entity]] = {
    val idSeq = ids.toSeq
    val batches = if (batchSize.isDefined) idSeq.grouped(batchSize.get) else Seq(idSeq)

    def query(idSeq: Seq[Id]): Future[Map[Id, Entity]] = {
      val query = keyspace.prepareQuery(mainFamily.columnFamily).getKeySlice(idSeq)
      query.executeAsync().map { res =>
        val loaded = for (row <- res.getResult) yield {
          row.getKey -> entityMapper.read(row.getKey, row.getColumns)
        }
        loaded.collect { case (id, Some(entity)) => id -> entity }.toMap
      }
    }

    val init = Future.successful(Map.empty[Id, Entity])
    batches.foldLeft(init) { (future, idsBatch) =>
      future.flatMap { accum => query(idsBatch).map(entities => accum ++ entities) }
    }
  }

  /**
   * Persist a given entity with a given id. Annotated @Id field is ignored.
   *
   * @param id target id
   * @param entity target entity
   * @return unit future
   */
  protected def mapperPersist(id: Id, entity: Entity): Future[Unit] = {
    val mutationBatch = keyspace.prepareMutationBatch()
    val rowMutation = mutationBatch.withRow(mainFamily.columnFamily, id)
    entityMapper.write(id, entity, rowMutation)
    mutationBatch.executeAsync().map { _ => Unit }
  }

  /**
   * Removes entity with target id.
   *
   * @param id target id
   * @return unit future
   */
  protected def mapperRemove(id: Id): Future[Unit] = {
    val mutationBatch = keyspace.prepareMutationBatch()
    mutationBatch.deleteRow(Seq(mainFamily.columnFamily), id)
    mutationBatch.executeAsync().map { _ => Unit }
  }
}
