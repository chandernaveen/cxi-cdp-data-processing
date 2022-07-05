package com.cxi.cdp.data_processing
package refined_zone.throtle

import support.tags.RequiresDatabricksRemoteCluster
import support.BaseSparkBatchJobTest

import org.scalatest.BeforeAndAfterEach
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}

@RequiresDatabricksRemoteCluster(reason = "uses Delta table so you cannot execute it locally")
class RawRefinedThrotleTidAttJobIntegrationTest extends BaseSparkBatchJobTest with BeforeAndAfterEach {

    import spark.implicits._

    val destTable = generateUniqueTableName("integration_test_throtle_tid_att_table")

    test("Write final DF to the destination table") {

        // given
        // initial set of rows
        val tid_att1 = Seq(
            ("thr_id_01", "thr_hh_id_11", "250K+", "Geologist", "1M+", "S"),
            ("thr_id_02", "thr_hh_id_22", "90-99,999", "Engineer/Civil", "50K-100K", "A")
        ).toDF("throtle_id", "throtle_hhid", "income", "occupation_code", "home_value", "dwelling_type")

        // when
        RawRefinedThrotleTidAttJob.write(destTable, tid_att1)

        // then
        withClue("Saved tid_att values do not match") {
            val actual = spark
                .table(destTable)
                .select("throtle_id", "throtle_hhid", "income", "occupation_code", "home_value", "dwelling_type")
            val expected = Seq(
                ("thr_id_01", "thr_hh_id_11", "250K+", "Geologist", "1M+", "S"),
                ("thr_id_02", "thr_hh_id_22", "90-99,999", "Engineer/Civil", "50K-100K", "A")
            ).toDF("throtle_id", "throtle_hhid", "income", "occupation_code", "home_value", "dwelling_type")
            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }

        // when
        // write initial set of rows one more time - emulate job re-run
        RawRefinedThrotleTidAttJob.write(destTable, tid_att1)

        // then
        withClue("Saved tid_att values do not match") {
            val actual = spark
                .table(destTable)
                .select("throtle_id", "throtle_hhid", "income", "occupation_code", "home_value", "dwelling_type")
            val expected = Seq(
                ("thr_id_01", "thr_hh_id_11", "250K+", "Geologist", "1M+", "S"),
                ("thr_id_02", "thr_hh_id_22", "90-99,999", "Engineer/Civil", "50K-100K", "A")
            ).toDF("throtle_id", "throtle_hhid", "income", "occupation_code", "home_value", "dwelling_type")
            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }

        // given
        // modify existing row, add new row
        val tid_att2 = Seq(
            ("thr_id_01", "thr_hh_id_11", "250K+", "Scientist", "1M+", "S"),
            ("thr_id_03", "thr_hh_id_33", "70-79,999", "Editor", "175 - 199,999", "U")
        ).toDF("throtle_id", "throtle_hhid", "income", "occupation_code", "home_value", "dwelling_type")

        // when
        RawRefinedThrotleTidAttJob.write(destTable, tid_att2)

        // then
        withClue("Saved tid_att values do not match") {
            val actual = spark
                .table(destTable)
                .select("throtle_id", "throtle_hhid", "income", "occupation_code", "home_value", "dwelling_type")
            val expected = Seq(
                ("thr_id_01", "thr_hh_id_11", "250K+", "Scientist", "1M+", "S"),
                ("thr_id_02", "thr_hh_id_22", "90-99,999", "Engineer/Civil", "50K-100K", "A"),
                ("thr_id_03", "thr_hh_id_33", "70-79,999", "Editor", "175 - 199,999", "U")
            ).toDF("throtle_id", "throtle_hhid", "income", "occupation_code", "home_value", "dwelling_type")
            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }

    }

    override protected def beforeEach(): Unit = {
        super.beforeEach()
        createTempTable(destTable)
    }

    override protected def afterEach(): Unit = {
        super.afterEach()
        dropTempTable(destTable)
    }

    def createTempTable(tableName: String): Unit = {
        spark.sql(s"""
               |CREATE TABLE IF NOT EXISTS $tableName
               (
               |    `throtle_id` STRING NOT NULL ,
               |    `throtle_hhid` STRING,
               |    `income` STRING,
               |    `occupation_code` STRING,
               |    `home_value` STRING,
               |    `dwelling_type` STRING
               |) USING delta
               |""".stripMargin)
    }

    def dropTempTable(tableName: String): Unit = {
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }

}
