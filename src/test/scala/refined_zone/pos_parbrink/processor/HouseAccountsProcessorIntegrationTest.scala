package com.cxi.cdp.data_processing
package refined_zone.pos_parbrink.processor

import refined_zone.pos_parbrink.processor.HouseAccountsProcessorTest.HouseAccountRefined
import support.tags.RequiresDatabricksRemoteCluster
import support.BaseSparkBatchJobTest

import org.scalatest.BeforeAndAfterEach

@RequiresDatabricksRemoteCluster(reason = "Uses delta table so can not be executed locally")
class HouseAccountsProcessorIntegrationTest extends BaseSparkBatchJobTest with BeforeAndAfterEach {

    val destTable = generateUniqueTableName("integration_test_parbrink_house_accounts_processor")

    test("write Parbrink house accounts") {
        import spark.implicits._

        // given
        val cxiPartnerId = "cxi-usa-partner-1"

        val houseAccount_A = HouseAccountRefined(
            cxi_partner_id = cxiPartnerId,
            location_id = "loc_id_1",
            house_account_id = 1,
            account_number = "1a",
            address_1 = "750 N. St. Paul Street",
            address_2 = "3rd Floor",
            balance = 123.45,
            city = "Dallas",
            email_address = "hashed_email_1",
            is_enforced_limit = true,
            first_name = "John",
            last_name = "Smith",
            limit = 499.99,
            name = "John Smith",
            phone_number = "hashed_phone_1",
            state = "TX",
            zip = "75201"
        )
        val houseAccounts_1 = Seq(houseAccount_A).toDF()

        // when
        // write houseAccounts_1 first time
        HouseAccountsProcessor.writeHouseAccounts(houseAccounts_1, cxiPartnerId, destTable)

        // then
        withClue("Saved house accounts do not match") {
            val actual = spark.table(destTable)
            assert("Column size not Equal", houseAccounts_1.columns.size, actual.columns.size)
            assertDataFrameDataEquals(houseAccounts_1, actual)
        }

        // when
        // write houseAccounts_1 one more time
        HouseAccountsProcessor.writeHouseAccounts(houseAccounts_1, cxiPartnerId, destTable)

        // then
        // no duplicates
        withClue("Saved houseAccounts do not match") {
            val actual = spark.table(destTable)
            assert("Column size not Equal", houseAccounts_1.columns.size, actual.columns.size)
            assertDataFrameDataEquals(houseAccounts_1, actual)
        }

        // given
        val houseAccount_A_modified = houseAccount_A.copy(address_2 = "4th Floor")
        val houseAccount_B = HouseAccountRefined(
            cxi_partner_id = cxiPartnerId,
            location_id = "loc_id_1",
            house_account_id = 2,
            account_number = "2b",
            address_1 = "Washington Blvd",
            address_2 = "201 W",
            balance = 345.1,
            city = "Los Angeles",
            email_address = "hashed_email_2",
            is_enforced_limit = false,
            first_name = "Paul",
            last_name = "O'Sullivan",
            limit = 0,
            name = "Paul O'Sullivan",
            phone_number = "hashed_phone_2",
            state = "CA",
            zip = "90007"
        )
        val houseAccounts_2 = Seq(houseAccount_A_modified, houseAccount_B).toDF()

        // when
        // write modified houseAccount_A and new houseAccount_B
        HouseAccountsProcessor.writeHouseAccounts(houseAccounts_2, cxiPartnerId, destTable)

        // then
        // houseAccount_A updated, houseAccount_B added
        withClue("Saved houseAccounts do not match") {
            val actual = spark.table(destTable)
            assert("Column size not Equal", houseAccounts_2.columns.size, actual.columns.size)
            assertDataFrameDataEquals(houseAccounts_2, actual)
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
               |    `cxi_partner_id`    STRING,
               |    `location_id`    STRING,
               |    `house_account_id`  INT,
               |    `account_number`    STRING,
               |    `address_1`         STRING,
               |    `address_2`         STRING,
               |    `address_3`         STRING,
               |    `address_4`         STRING,
               |    `balance`           DECIMAL(9, 2),
               |    `city`              STRING,
               |    `email_address`     STRING,
               |    `is_enforced_limit` BOOLEAN,
               |    `first_name`        STRING,
               |    `last_name`         STRING,
               |    `limit`             DECIMAL(9, 2),
               |    `middle_name`       STRING,
               |    `name`              STRING,
               |    `phone_number`      STRING,
               |    `state`             STRING,
               |    `zip`               STRING
               |) USING delta
               |PARTITIONED BY (cxi_partner_id);
               |""".stripMargin)
    }

    def dropTempTable(tableName: String): Unit = {
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }

}
