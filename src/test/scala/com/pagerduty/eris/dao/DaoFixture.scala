package com.pagerduty.eris.dao

import com.netflix.astyanax.connectionpool.NodeDiscoveryType
import com.netflix.astyanax.connectionpool.impl.{ConnectionPoolConfigurationImpl, CountingConnectionPoolMonitor}
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl
import com.netflix.astyanax.{Cluster, Keyspace}
import com.pagerduty.eris.ClusterCtx
import com.pagerduty.eris.schema.SchemaLoader
import org.scalatest.{Outcome, fixture}


trait DaoFixture { this: fixture.Suite =>
  type FixtureParam <: Dao
  protected def mkFixtureDao(cluster: Cluster, keyspace: Keyspace): FixtureParam

  def withFixture(test: OneArgTest): Outcome = {
    val context = new ClusterCtx(
      clusterName = "CassCluster",
      astyanaxConfig = new AstyanaxConfigurationImpl()
        .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE),
      connectionPoolConfig = new ConnectionPoolConfigurationImpl("CassConnectionPool")
        .setSeeds("localhost:9160")
        .setPort(9160),
      connectionPoolMonitor = new CountingConnectionPoolMonitor()
    )
    val cluster = context.cluster

    val keyspaceName = getClass.getSimpleName + Thread.currentThread.getId
    val keyspace = cluster.getKeyspace(keyspaceName)
    val dao = mkFixtureDao(cluster, keyspace)
    val schemaLoader = new SchemaLoader(cluster, dao.asInstanceOf[Dao].columnFamilyDefs)

    try {
      schemaLoader.loadSchema()
      withFixture(test.toNoArgTest(dao))
    } finally {
      schemaLoader.dropSchema()
    }
  }
}
