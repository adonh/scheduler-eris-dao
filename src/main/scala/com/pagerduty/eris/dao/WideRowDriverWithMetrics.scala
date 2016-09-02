package com.pagerduty.eris.dao

import com.pagerduty.eris.ColumnFamilyModel
import com.pagerduty.eris.widerow.WideRowDriverImpl
import com.pagerduty.widerow.{ Entry, EntryColumn }
import com.pagerduty.metrics.Stopwatch
import scala.concurrent.{ Future, ExecutionContextExecutor }

class WideRowDriverWithMetrics[RowKey, ColName, ColValue](
  columnFamilyModel: ColumnFamilyModel[RowKey, ColName, ColValue],
  executor: ExecutionContextExecutor,
  settings: ErisSettings
)
    extends WideRowDriverImpl(columnFamilyModel, executor) {
  override def fetchData(
    rowKey: RowKey,
    ascending: Boolean,
    from: Option[ColName],
    to: Option[ColName],
    limit: Int
  ): Future[IndexedSeq[Entry[RowKey, ColName, ColValue]]] = {
    val stopwatch = Stopwatch.start()
    val result = super.fetchData(rowKey, ascending, from, to, limit)
    val decorated = Interceptor.decorate(
      columnFamilyModel.name, "index_load_page", QueryType.Read, settings.metrics
    )(stopwatch, result)

    decorated
  }

  override def update(
    rowKey: RowKey,
    remove: Iterable[ColName],
    insert: Iterable[EntryColumn[ColName, ColValue]]
  ): Future[Unit] = {
    val stopwatch = Stopwatch.start()
    val result = super.update(rowKey, remove, insert)
    val decorated = Interceptor.decorate(
      columnFamilyModel.name, "index_update", QueryType.Write, settings.metrics
    )(stopwatch, result)

    decorated
  }

  override def deleteRow(rowKey: RowKey): Future[Unit] = {
    val stopwatch = Stopwatch.start()
    val result = super.deleteRow(rowKey)
    val decorated = Interceptor.decorate(
      columnFamilyModel.name, "delete_row", QueryType.Write, settings.metrics
    )(stopwatch, result)

    decorated
  }
}
