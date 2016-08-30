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

import com.netflix.astyanax.ddl.ColumnFamilyDefinition
import com.netflix.astyanax.{ Cluster, Keyspace, Serializer }
import com.pagerduty.eris._
import scala.concurrent.ExecutionContextExecutor

/**
 * Base of DAO hierarchy, contains methods to define schema and integrate with the
 * schema loader.
 */
trait Dao {
  protected val cluster: Cluster
  protected val keyspace: Keyspace

  /**
   * Custom config that will be combined with reasonable fallbacks.
   *
   * @return config
   */
  protected def settings: ErisSettings = new ErisSettings

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
    colValueSerializer: Serializer[ColValue]): ColumnFamilyModel[RowKey, ColName, ColValue] =
    {
      val model = ColumnFamilyModel(keyspace, name, settings, columns)(
        rowKeySerializer, colNameSerializer, colValueSerializer
      )
      _columnFamilyDefs += model.columnFamilyDef(cluster)
      model
    }

  /**
   * Returns column family definitions used to build Cassandra schema.
   * @return column family definitions
   */
  def columnFamilyDefs: Set[ColumnFamilyDefinition] = _columnFamilyDefs
  private var _columnFamilyDefs = Set.empty[ColumnFamilyDefinition]

  /**
   * Shared executor that will be used when working with Futures.
   *
   * @return executor
   */
  protected implicit def executor: ExecutionContextExecutor

  /**
   * Implements basic WideRowIndex.
   */
  class WideRowIndex[RowKey, ColName, ColValue](
    val columnFamilyModel: ColumnFamilyModel[RowKey, ColName, ColValue],
    pageSize: Int = 100
  )
      extends com.pagerduty.widerow.WideRowIndex[RowKey, ColName, ColValue](
        new WideRowDriverWithMetrics(columnFamilyModel, executor, settings),
        pageSize
      )

  /**
   * Implements basic WideRowMap.
   */
  class WideRowMap[RowKey, ColName, ColValue](
    val columnFamilyModel: ColumnFamilyModel[RowKey, ColName, ColValue],
    pageSize: Int = 100
  )
      extends com.pagerduty.widerow.WideRowMap[RowKey, ColName, ColValue](
        new WideRowDriverWithMetrics(columnFamilyModel, executor, settings),
        pageSize
      )

  /**
   * Implements basic WideRowSet.
   */
  class WideRowSet[RowKey, ColName](
    val columnFamilyModel: ColumnFamilyModel[RowKey, ColName, Array[Byte]],
    pageSize: Int = 100
  )
      extends com.pagerduty.widerow.WideRowSet[RowKey, ColName](
        new WideRowDriverWithMetrics(columnFamilyModel, executor, settings),
        pageSize
      )
}
