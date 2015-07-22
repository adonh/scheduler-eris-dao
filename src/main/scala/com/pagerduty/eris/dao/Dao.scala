package com.pagerduty.eris.dao

import com.netflix.astyanax.ddl.ColumnFamilyDefinition
import com.netflix.astyanax.{Cluster, Keyspace, Serializer}
import com.pagerduty.eris._


/**
 * Base of DAO hierarchy, contains methods to define schema and integrate with the
 * schema loader.
 */
trait Dao {
  protected val cluster: Cluster
  protected val keyspace: Keyspace

  /**
   * Creates column family model and retains related schema.
   */
  protected def columnFamily[RowKey, ColName, ColValue](
      name: String,
      settings: ColumnFamilySettings = new ColumnFamilySettings,
      columns: Set[ColumnModel] = Set.empty
    )(implicit
      rowKeySerializer: Serializer[RowKey],
      colNameSerializer: Serializer[ColName],
      colValueSerializer: Serializer[ColValue])
  : ColumnFamilyModel[RowKey, ColName, ColValue] =
  {
    val model = ColumnFamilyModel(keyspace, name, settings, columns)(
      rowKeySerializer, colNameSerializer, colValueSerializer)
    _columnFamilyDefs += model.columnFamilyDef(cluster)
    model
  }

  private var _columnFamilyDefs = Set.empty[ColumnFamilyDefinition]
  def columnFamilyDefs: Set[ColumnFamilyDefinition] = _columnFamilyDefs
}
