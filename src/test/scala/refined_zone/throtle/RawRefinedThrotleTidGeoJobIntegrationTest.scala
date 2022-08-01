package com.cxi.cdp.data_processing
package refined_zone.throtle
import support.tags.RequiresDatabricksRemoteCluster
import support.BaseSparkBatchJobTest

import org.apache.spark.sql.types.{DataTypes, StringType, StructType}
import org.apache.spark.sql.Row
import org.scalatest.BeforeAndAfterEach
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}

@RequiresDatabricksRemoteCluster(reason = "uses Delta table so you cannot execute it locally")
class RawRefinedThrotleTidGeoJobIntegrationTest extends BaseSparkBatchJobTest with BeforeAndAfterEach {

    val destTable = generateUniqueTableName("tid_geo_test")

    test("Write final DF to the destination table") {

        // given
        val rawStruct = new StructType()
            .add("throtle_id", StringType)
            .add("throtle_hhid", StringType)
            .add("zip_code", StringType, false)

        val tid_geos_final = Seq(
            Row("49688311", "299649563", "43344"),
            Row("13227693185", null, "57701")
        )

        import collection.JavaConverters._
        val tid_geos_final_df = spark.createDataFrame(tid_geos_final.asJava, rawStruct)

        // when
        RawRefinedThrotleTidGeoJob.writeThrotleGeos(tid_geos_final_df, destTable)

        // then
        withClue("First time writing to table") {
            val actual = spark
                .table(destTable)

            val expected = tid_geos_final_df

            import collection.JavaConverters._
            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }

        // when
        RawRefinedThrotleTidGeoJob.writeThrotleGeos(tid_geos_final_df, destTable)

        // then
        withClue("Rewriting initial set of rows - emulate job re-run") {
            val actual = spark
                .table(destTable)

            val expected = tid_geos_final_df

            import collection.JavaConverters._
            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }

        // given
        // modify existing row, add new row
        val tid_geos_final_2 = Seq(
            Row("49688311", "299649563", "hellothere"),
            Row("12345", "23421", "57701")
        )

        import collection.JavaConverters._
        val tid_geos_final_df_2 = spark.createDataFrame(tid_geos_final_2.asJava, rawStruct)

        // when
        RawRefinedThrotleTidGeoJob.writeThrotleGeos(tid_geos_final_df_2, destTable)

        // then
        withClue("Update and insert row - zip should overwrite with new") {
            val actual = spark
                .table(destTable)
            val expected_data = Seq(
                Row("49688311", "299649563", "hellothere"),
                Row("13227693185", null, "57701"),
                Row("12345", "23421", "57701")
            )

            import collection.JavaConverters._
            val expected = spark.createDataFrame(expected_data.asJava, rawStruct)
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
                   |(
                   |    `throtle_id`                string not null,
                   |    `throtle_hhid`              string,
                   |    `zip_code`                  STRING not null
                   |) USING delta;
                   |""".stripMargin)
    }

    def dropTempTable(tableName: String): Unit = {
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
}
