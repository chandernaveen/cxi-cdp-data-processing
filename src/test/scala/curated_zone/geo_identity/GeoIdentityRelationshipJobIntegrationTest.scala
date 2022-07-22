package com.cxi.cdp.data_processing
package curated_zone.geo_identity

import curated_zone.geo_identity.model.{GeoIdentityRow, GeoLocationToPosCustomer360Row}
import refined_zone.hub.identity.model.IdentityRelationship
import support.tags.RequiresDatabricksRemoteCluster
import support.utils.DateTimeTestUtils.sqlDate
import support.BaseSparkBatchJobTest

import org.scalatest.{BeforeAndAfterEach, Matchers}

@RequiresDatabricksRemoteCluster(reason = "Uses delta table so can not be executed locally")
class GeoIdentityRelationshipJobIntegrationTest extends BaseSparkBatchJobTest with Matchers with BeforeAndAfterEach {

    import GeoIdentityRelationshipJob._

    private val geoLocationToCustomerTable = generateUniqueTableName(
        "integration_test_geo_location_to_pos_customer_360"
    )
    private val geoIdentityTable = generateUniqueTableName("integration_test_geo_identity")
    private val geoIdentityRelationshipTable = generateUniqueTableName("integration_test_geo_identity_relationship")

    test("writeGeoLocationToCustomer") {
        import spark.implicits._

        createGeoLocationToCustomerTable(geoLocationToCustomerTable)
        try {
            // first test - with an empty table

            val firstBatch = Seq(
                GeoLocationToPosCustomer360Row(
                    maid = "maid_1",
                    maid_type = "maid_type_1",
                    customer_360_id = "customer_1",
                    frequency_linked = Some(3),
                    profile_maid_link_score = Some(0.5),
                    total_profiles_per_maid = Some(2L),
                    total_score_per_maid = Some(1.5),
                    total_maids_per_profile = Some(2L),
                    total_score_per_profile = Some(1.2),
                    confidence_score = Some(0.0)
                )
            ).toDF

            writeGeoLocationToCustomer(firstBatch, geoLocationToCustomerTable)(spark)

            assertDataFrameDataEquals(firstBatch, spark.table(geoLocationToCustomerTable))

            // second test - with a table that has an existing record - should overwrite old records

            val secondBatch = Seq(
                GeoLocationToPosCustomer360Row(
                    maid = "maid_2",
                    maid_type = "maid_type_2",
                    customer_360_id = "customer_2",
                    frequency_linked = Some(5),
                    profile_maid_link_score = Some(2.0),
                    total_profiles_per_maid = Some(1L),
                    total_score_per_maid = Some(5.0),
                    total_maids_per_profile = Some(2L),
                    total_score_per_profile = Some(10.0),
                    confidence_score = Some(0.65)
                )
            ).toDF

            writeGeoLocationToCustomer(secondBatch, geoLocationToCustomerTable)(spark)

            assertDataFrameDataEquals(secondBatch, spark.table(geoLocationToCustomerTable))
        } finally {
            dropTable(geoLocationToCustomerTable)
        }
    }

    test("writeGeoIdentity") {
        import spark.implicits._

        createGeoIdentityTable(geoIdentityTable)
        try {
            // first test - with an empty table

            val firstBatch = Seq(
                GeoIdentityRow(
                    cxi_identity_id = "maid_1",
                    `type` = "maid_type_1",
                    weight = "0.3",
                    metadata = Map("a" -> "b")
                )
            )

            writeGeoIdentity(firstBatch.toDF, geoIdentityTable)(spark)

            spark.table(geoIdentityTable).as[GeoIdentityRow].collect() should contain theSameElementsAs firstBatch

            // second test - with a table that has an existing record - should overwrite old records

            val secondBatch = Seq(
                GeoIdentityRow(
                    cxi_identity_id = "maid_2",
                    `type` = "maid_type_2",
                    weight = null,
                    metadata = null
                )
            )

            writeGeoIdentity(secondBatch.toDF, geoIdentityTable)(spark)

            spark.table(geoIdentityTable).as[GeoIdentityRow].collect() should contain theSameElementsAs secondBatch
        } finally {
            dropTable(geoIdentityTable)
        }
    }

    test("writeGeoIdentityRelationship") {
        import spark.implicits._

        createGeoIdentityRelationshipTable(geoIdentityRelationshipTable)
        try {
            val records = Seq(
                IdentityRelationship(
                    source = "maid_1",
                    source_type = "maid_type_1",
                    target = "111",
                    target_type = "phone",
                    relationship = RelationshipType,
                    frequency = 3,
                    created_date = sqlDate("2022-06-10"),
                    last_seen_date = sqlDate("2022-06-10"),
                    active_flag = true
                )
            ).toDF

            writeGeoLocationToCustomer(records, geoIdentityRelationshipTable)(spark)

            assertDataFrameDataEquals(records, spark.table(geoIdentityRelationshipTable))
        } finally {
            dropTable(geoIdentityRelationshipTable)
        }
    }

    private def createGeoLocationToCustomerTable(tableName: String): Unit = {
        spark.sql(s"""
            CREATE TABLE IF NOT EXISTS `$tableName` (
                `maid` STRING,
                `maid_type` STRING,
                `customer_360_id` STRING,
                `frequency_linked` INT,
                `profile_maid_link_score` DOUBLE,
                `total_profiles_per_maid` BIGINT,
                `total_score_per_maid` DOUBLE,
                `total_maids_per_profile` BIGINT,
                `total_score_per_profile` DOUBLE,
                `confidence_score` DOUBLE)
            USING delta;
       """)
    }

    private def createGeoIdentityTable(tableName: String): Unit = {
        spark.sql(s"""
            CREATE TABLE IF NOT EXISTS `$tableName` (
                `cxi_identity_id` STRING,
                `type` STRING,
                `weight` STRING,
                `metadata` MAP<STRING, STRING>)
            USING delta;
       """)
    }

    private def createGeoIdentityRelationshipTable(tableName: String): Unit = {
        spark.sql(s"""
            CREATE TABLE IF NOT EXISTS `$tableName` (
              `source` STRING,
              `source_type` STRING,
              `target` STRING,
              `target_type` STRING,
              `relationship` STRING,
              `frequency` INT,
              `created_date` DATE,
              `last_seen_date` DATE,
              `active_flag` BOOLEAN)
            USING delta
            PARTITIONED BY (relationship);
       """)
    }

    private def dropTable(tableName: String): Unit = {
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
}
