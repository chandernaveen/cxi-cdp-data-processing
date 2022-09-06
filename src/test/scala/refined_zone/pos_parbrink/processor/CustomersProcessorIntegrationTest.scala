package com.cxi.cdp.data_processing
package refined_zone.pos_parbrink.processor

import refined_zone.pos_parbrink.processor.CustomersProcessorTest.CustomerRefined
import support.tags.RequiresDatabricksRemoteCluster
import support.utils.DateTimeTestUtils.sqlTimestamp
import support.BaseSparkBatchJobTest

import org.scalatest.BeforeAndAfterEach

@RequiresDatabricksRemoteCluster(reason = "Uses delta table so can not be executed locally")
class CustomersProcessorIntegrationTest extends BaseSparkBatchJobTest with BeforeAndAfterEach {

    val destTable = generateUniqueTableName("integration_test_parbrink_customers_processor")

    test("write Parbrink Customers") {
        import spark.implicits._

        // given
        val cxiPartnerId = "cxi-usa-partner-1"

        val customer_A = CustomerRefined(
            customer_id = "customer_1",
            email_address = "hashed_email_1",
            first_name = "Alane",
            last_name = "First",
            created_at = sqlTimestamp("2020-05-12T17:27:14.03Z"),
            phone_numbers = Seq("hashed_phone_1"),
            cxi_partner_id = cxiPartnerId,
            version = null
        )
        val customers_1 = Seq(customer_A).toDF()

        // when
        // write customers_1 first time
        CustomersProcessor.writeCustomers(customers_1, cxiPartnerId, destTable)

        // then
        withClue("Saved Customers do not match") {
            val actual = spark.table(destTable)
            assert("Column size not Equal", customers_1.columns.size, actual.columns.size)
            assertDataFrameDataEquals(customers_1, actual)
        }

        // when
        // write Customers_1 one more time
        CustomersProcessor.writeCustomers(customers_1, cxiPartnerId, destTable)

        // then
        // no duplicates
        withClue("Saved Customers do not match") {
            val actual = spark.table(destTable)
            assert("Column size not Equal", customers_1.columns.size, actual.columns.size)
            assertDataFrameDataEquals(customers_1, actual)
        }

        // given
        val customer_A_modified = customer_A.copy(first_name = "Alan")
        val customer_B = CustomerRefined(
            customer_id = "customer_2",
            email_address = "hashed_email_2",
            first_name = "Darryl",
            last_name = "Second",
            created_at = sqlTimestamp("2020-05-01T22:58:09.183Z"),
            phone_numbers = Seq("hashed_phone_2", "hashed_phone_3"),
            cxi_partner_id = cxiPartnerId,
            version = null
        )

        val customers_2 = Seq(customer_A_modified, customer_B).toDF()

        // when
        // write modified customer_A and new customer_B
        CustomersProcessor.writeCustomers(customers_2, cxiPartnerId, destTable)

        // then
        // customer_A updated, customer_B added
        withClue("Saved Customers do not match") {
            val actual = spark.table(destTable)
            assert("Column size not Equal", customers_2.columns.size, actual.columns.size)
            assertDataFrameDataEquals(customers_2, actual)
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
               |CREATE TABLE $tableName
               |(
               |    `cxi_partner_id` STRING,
               |    `customer_id`    STRING,
               |    `email_address`  STRING,
               |    `phone_numbers`  ARRAY<STRING>,
               |    `first_name`     STRING,
               |    `last_name`      STRING,
               |    `created_at`     TIMESTAMP,
               |    `version`        STRING
               |) USING delta
               |PARTITIONED BY (cxi_partner_id);
               |""".stripMargin)
    }

    def dropTempTable(tableName: String): Unit = {
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }

}
