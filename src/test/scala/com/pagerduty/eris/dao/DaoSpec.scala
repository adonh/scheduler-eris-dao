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

import java.util.Date

import com.netflix.astyanax.serializers.ComparatorType
import com.pagerduty.eris.{TimeUuid, ColumnModel, ColumnFamilySettings, TestClusterCtx}
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
        val cluster = TestClusterCtx.cluster
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
