package com.pagerduty.eris.dao

import java.util.Date

import com.netflix.astyanax.serializers.ComparatorType
import com.pagerduty.eris.{TimeUuid, ColumnModel, ColumnFamilySettings, TestCluster}
import com.pagerduty.eris.serializers._
import org.scalatest.{FreeSpec, Matchers}


class DaoSpec extends FreeSpec with Matchers {

  "Dao should" - {
    "keep track of declared column families" in {
      val keyspaceName = "DaoSpec"
      val simpleCfName = "simpleCf"
      val indexedCfName = "indexedCf"
      val counterCfName = "counterCf"
      val indexColName = "indexCol"

      val dao = new Dao {
        val cluster = TestCluster.cluster
        val keyspace = cluster.getKeyspace(keyspaceName)

        columnFamily[TimeUuid, Date, String](simpleCfName)
        columnFamily[String, Int, String](indexedCfName,
          columns = Set(ColumnModel[Boolean](indexColName, indexed = true)))
        columnFamily[String, String, Long](counterCfName, new ColumnFamilySettings(
          colValueValidatorOverride = Some(ComparatorType.COUNTERTYPE.getClassName)))
      }

      dao.columnFamilyDefs.size shouldBe 3
      dao.columnFamilyDefs.forall(_.getKeyspace == keyspaceName) shouldBe true

      val simpleCfDef = dao.columnFamilyDefs.find(_.getName == simpleCfName).get
      simpleCfDef.getKeyValidationClass shouldBe ValidatorClass[TimeUuid]
      simpleCfDef.getComparatorType shouldBe ValidatorClass[Date]
      simpleCfDef.getDefaultValidationClass shouldBe ValidatorClass[String]
      simpleCfDef.getColumnDefinitionList shouldBe empty

      val indexedCfDef = dao.columnFamilyDefs.find(_.getName == indexedCfName).get
      indexedCfDef.getKeyValidationClass shouldBe ValidatorClass[String]
      indexedCfDef.getComparatorType shouldBe ValidatorClass[Int]
      indexedCfDef.getDefaultValidationClass shouldBe ValidatorClass[String]
      indexedCfDef.getColumnDefinitionList.size shouldBe 1
      val indexedCfCol = indexedCfDef.getColumnDefinitionList.get(0)
      indexedCfCol.getName shouldBe indexColName
      indexedCfCol.getValidationClass shouldBe ValidatorClass[Boolean]
      indexedCfCol.hasIndex shouldBe true

      val counterCfDef = dao.columnFamilyDefs.find(_.getName == counterCfName).get
      counterCfDef.getKeyValidationClass shouldBe ValidatorClass[String]
      counterCfDef.getComparatorType shouldBe ValidatorClass[String]
      counterCfDef.getDefaultValidationClass shouldBe ComparatorType.COUNTERTYPE.getClassName
      counterCfDef.getColumnDefinitionList shouldBe empty
    }
  }
}
