package com.cxi.cdp.data_processing
package refined_zone.segmint

import support.tags.RequiresDatabricksRemoteCluster
import support.BaseSparkBatchJobTest

import org.scalatest.BeforeAndAfterEach

@RequiresDatabricksRemoteCluster(reason = "Uses delta table so can not be executed locally")
class RawRefinedSegmintIntegrationTest extends BaseSparkBatchJobTest with BeforeAndAfterEach {

    val destTable = generateUniqueTableName("integration_test_segmint_postal_merch")

    test("Write to refined Segmint table") {
        import spark.implicits._

        // given
        // initial set of data
        val segmintData_1 = Seq(
            (
                "2022-09-11",
                "MCDONALD'S",
                "FAST FOOD RESTAURANTS",
                "QSR",
                "American",
                "AK",
                "99504",
                10,
                2.99,
                "City 1",
                "Region 1"
            ),
            (
                "2022-09-11",
                "MOOSE'S TOOTH",
                "PIZZA PARLORS",
                "Fast Casual",
                "Italian",
                "AK",
                "99502",
                1,
                1.99,
                "City 3",
                "Region 1"
            ),
            (
                "2022-09-11",
                "STARBUCKS",
                "COFFEE SHOPS",
                "QSR",
                "American",
                "AK",
                "99503",
                5,
                5.99,
                "City 2",
                "Region 1"
            ),
            (
                "2022-09-11",
                "TACO BELL",
                "MEXICAN RESTAURANTS",
                "Other",
                "Other",
                "AK",
                "99504",
                2,
                3.0,
                "City 1",
                "Region 1"
            ),
            (
                "2022-09-18",
                "MCDONALD'S",
                "FAST FOOD RESTAURANTS",
                "QSR",
                "American",
                "AK",
                "99504",
                10,
                5.99,
                "City 1",
                "Region 1"
            ),
            (
                "2022-09-18",
                "STARBUCKS",
                "COFFEE SHOPS",
                "QSR",
                "American",
                "AK",
                "99503",
                4,
                5.99,
                "City 2",
                "Region 1"
            ),
            ("2022-09-25", "STARBUCKS", "COFFEE SHOPS", "QSR", "American", "AK", "99503", 3, 5.99, "City 2", "Region 1")
        ).toDF(
            "date",
            "merchant",
            "location_type",
            "industry_category",
            "cuisine_category",
            "state",
            "postal_code",
            "transaction_quantity",
            "transaction_amount",
            "city",
            "region"
        )

        // when
        // write segmintData_1 first time
        RawRefinedSegmintJob.writeSegmint(segmintData_1, destTable)

        // then
        withClue("Saved Segmint data does not match") {
            val actual = spark
                .table(destTable)
                .select(
                    "date",
                    "merchant",
                    "location_type",
                    "industry_category",
                    "cuisine_category",
                    "state",
                    "postal_code",
                    "transaction_quantity",
                    "transaction_amount",
                    "city",
                    "region"
                )
            assertDataFrameDataEquals(segmintData_1, actual)
        }

        // when
        // write the same segmintData_1 second time - shall not have duplicates
        RawRefinedSegmintJob.writeSegmint(segmintData_1, destTable)

        // then
        withClue("Saved Segmint data does not match") {
            val actual = spark
                .table(destTable)
                .select(
                    "date",
                    "merchant",
                    "location_type",
                    "industry_category",
                    "cuisine_category",
                    "state",
                    "postal_code",
                    "transaction_quantity",
                    "transaction_amount",
                    "city",
                    "region"
                )
            assertDataFrameDataEquals(segmintData_1, actual)
        }

        // given
        val segmintData_2 = Seq(
            (
                "2022-09-11",
                "MCDONALD'S",
                "FAST FOOD RESTAURANTS",
                "QSR",
                "American",
                "AK",
                "99504",
                10,
                2.99,
                "City 1",
                "Region 2"
            ), // updated region
            (
                "2022-10-24",
                "MOOSE'S TOOTH",
                "PIZZA PARLORS",
                "Fast Casual",
                "Italian",
                "AK",
                "99502",
                3,
                4.99,
                "City 3",
                "Region 1"
            ) // added row
        ).toDF(
            "date",
            "merchant",
            "location_type",
            "industry_category",
            "cuisine_category",
            "state",
            "postal_code",
            "transaction_quantity",
            "transaction_amount",
            "city",
            "region"
        )

        // when
        // write segmintData_2 - 1 row is updated, 1 row is added
        RawRefinedSegmintJob.writeSegmint(segmintData_2, destTable)

        // then
        withClue("Saved Segmint data does not match") {
            val expected = Seq(
                (
                    "2022-09-11",
                    "MCDONALD'S",
                    "FAST FOOD RESTAURANTS",
                    "QSR",
                    "American",
                    "AK",
                    "99504",
                    10,
                    2.99,
                    "City 1",
                    "Region 2"
                ), // updated row
                (
                    "2022-09-11",
                    "MOOSE'S TOOTH",
                    "PIZZA PARLORS",
                    "Fast Casual",
                    "Italian",
                    "AK",
                    "99502",
                    1,
                    1.99,
                    "City 3",
                    "Region 1"
                ),
                (
                    "2022-09-11",
                    "STARBUCKS",
                    "COFFEE SHOPS",
                    "QSR",
                    "American",
                    "AK",
                    "99503",
                    5,
                    5.99,
                    "City 2",
                    "Region 1"
                ),
                (
                    "2022-09-11",
                    "TACO BELL",
                    "MEXICAN RESTAURANTS",
                    "Other",
                    "Other",
                    "AK",
                    "99504",
                    2,
                    3.0,
                    "City 1",
                    "Region 1"
                ),
                (
                    "2022-09-18",
                    "MCDONALD'S",
                    "FAST FOOD RESTAURANTS",
                    "QSR",
                    "American",
                    "AK",
                    "99504",
                    10,
                    5.99,
                    "City 1",
                    "Region 1"
                ),
                (
                    "2022-09-18",
                    "STARBUCKS",
                    "COFFEE SHOPS",
                    "QSR",
                    "American",
                    "AK",
                    "99503",
                    4,
                    5.99,
                    "City 2",
                    "Region 1"
                ),
                (
                    "2022-09-25",
                    "STARBUCKS",
                    "COFFEE SHOPS",
                    "QSR",
                    "American",
                    "AK",
                    "99503",
                    3,
                    5.99,
                    "City 2",
                    "Region 1"
                ),
                (
                    "2022-10-24",
                    "MOOSE'S TOOTH",
                    "PIZZA PARLORS",
                    "Fast Casual",
                    "Italian",
                    "AK",
                    "99502",
                    3,
                    4.99,
                    "City 3",
                    "Region 1"
                ) // added row
            ).toDF(
                "date",
                "merchant",
                "location_type",
                "industry_category",
                "cuisine_category",
                "state",
                "postal_code",
                "transaction_quantity",
                "transaction_amount",
                "city",
                "region"
            )
            val actual = spark
                .table(destTable)
                .select(
                    "date",
                    "merchant",
                    "location_type",
                    "industry_category",
                    "cuisine_category",
                    "state",
                    "postal_code",
                    "transaction_quantity",
                    "transaction_amount",
                    "city",
                    "region"
                )
            assertDataFrameDataEquals(expected, actual)
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
               CREATE TABLE IF NOT EXISTS $tableName
               (
                   `date` DATE,
                   `merchant` STRING,
                   `location_type` STRING,
                   `industry_category` STRING,
                   `cuisine_category` STRING,
                   `state` STRING,
                   `postal_code` STRING,
                   `transaction_quantity` INT,
                   `transaction_amount` DECIMAL(9,2),
                   `city` STRING,
                   `region` STRING
               ) USING delta PARTITIONED BY (date);
               """.stripMargin)
    }

    def dropTempTable(tableName: String): Unit = {
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }

}
