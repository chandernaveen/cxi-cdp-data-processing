package com.cxi.cdp.data_processing
package curated_zone.signal_framework.transactional_insights

import support.BaseSparkBatchJobTest
import support.tags.RequiresDatabricksRemoteCluster

import org.scalatest.BeforeAndAfterEach
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}

@RequiresDatabricksRemoteCluster(reason = "uses Delta table so you cannot execute it locally")
class TransactionalInsightsSignalsJobIntegrationTest extends BaseSparkBatchJobTest with BeforeAndAfterEach {

    import spark.implicits._
    val destTable = "integration_test_daily_signals_table"

    test("Write final DF to the destination table") {

        // given
        // initial set of signals 'partner-1 / order_metrics / total_orders / 2022-02-24'
        val signals_1 = Seq(
            ("cust-1", "partner-1", "loc-1", "order_metrics", "total_orders", 5, "time_period_7"),
            ("cust-2", "partner-1", "loc-1", "order_metrics", "total_orders", 3, "time_period_7")
        ).toDF("customer_360_id", "cxi_partner_id", "location_id", "signal_domain", "signal_name", "signal_value", "date_option")

        // when
        TransactionalInsightsSignalsJob.write(signals_1, destTable, "2022-02-24")

        //then
        withClue("Saved signals do not match") {
            val actual_1 = spark.table(destTable)
                .select("customer_360_id", "cxi_partner_id", "location_id", "signal_domain", "signal_name", "signal_value", "date_option", "signal_generation_date")
            val expected_1 = Seq(
                ("cust-1", "partner-1", "loc-1", "order_metrics", "total_orders", "5", "time_period_7", sqlDate("2022-02-24")),
                ("cust-2", "partner-1", "loc-1", "order_metrics", "total_orders", "3", "time_period_7", sqlDate("2022-02-24"))
            ).toDF("customer_360_id", "cxi_partner_id", "location_id", "signal_domain", "signal_name", "signal_value", "date_option", "signal_generation_date")
            actual_1.schema.fields.map(_.name) shouldEqual expected_1.schema.fields.map(_.name)
            actual_1.collect() should contain theSameElementsAs expected_1.collect()
        }

        // given
        // replace signals for 'partner-1 / order_metrics / total_orders / 2022-02-24'
        val signals_2 = Seq(
            ("cust-4", "partner-1", "loc-1", "order_metrics", "total_orders", 25, "time_period_7"),
            ("cust-5", "partner-2", "loc-1", "order_metrics", "total_orders", 17, "time_period_90")
        ).toDF("customer_360_id", "cxi_partner_id", "location_id", "signal_domain", "signal_name", "signal_value", "date_option")

        // insert a signal that pertains to some other domain (not transactional insights)
        // expect this signal not to be deleted after inserting TI signals with the same feed date
        spark.sql(
            s"""INSERT INTO $destTable PARTITION (signal_generation_date='2022-02-24', signal_domain, signal_name, cxi_partner_id)
               |(customer_360_id, cxi_partner_id, location_id, signal_domain, signal_name, signal_value, date_option)
               |VALUES ('cust-3', 'partner-1', 'loc-1', 'SOME_OTHER_DOMAIN', 'SOME_OTHER_SIGNAL', 'xyz', 'time_period_60')
               |""".stripMargin)

        // when
        TransactionalInsightsSignalsJob.write(signals_2, destTable, "2022-02-24")

        //then
        withClue("Saved signals do not match") {
            val actual_2 = spark.table(destTable)
                .select("customer_360_id", "cxi_partner_id", "location_id", "signal_domain", "signal_name", "signal_value", "date_option", "signal_generation_date")
            val expected_2 = Seq(
                ("cust-3", "partner-1", "loc-1", "SOME_OTHER_DOMAIN", "SOME_OTHER_SIGNAL", "xyz", "time_period_60", sqlDate("2022-02-24")),
                ("cust-4", "partner-1", "loc-1", "order_metrics", "total_orders", "25", "time_period_7", sqlDate("2022-02-24")),
                ("cust-5", "partner-2", "loc-1", "order_metrics", "total_orders", "17", "time_period_90", sqlDate("2022-02-24"))
            ).toDF("customer_360_id", "cxi_partner_id", "location_id", "signal_domain", "signal_name", "signal_value", "date_option", "signal_generation_date")
            actual_2.schema.fields.map(_.name) shouldEqual expected_2.schema.fields.map(_.name)
            actual_2.collect() should contain theSameElementsAs expected_2.collect()
        }

        // given
        // add new signals for feed date = '2022-02-25'
        val signals_3 = Seq(
            ("cust-4", "partner-1", "loc-1", "order_metrics", "total_orders", 123, "time_period_7"),
            ("cust-5", "partner-2", "loc-1", "order_metrics", "total_orders", 234, "time_period_90")
        ).toDF("customer_360_id", "cxi_partner_id", "location_id", "signal_domain", "signal_name", "signal_value", "date_option")

        // when
        TransactionalInsightsSignalsJob.write(signals_3, destTable, "2022-02-25")

        //then
        withClue("Saved signals do not match") {
            val actual_3 = spark.table(destTable)
                .select("customer_360_id", "cxi_partner_id", "location_id", "signal_domain", "signal_name", "signal_value", "date_option", "signal_generation_date")
            val expected_3 = Seq(
                ("cust-3", "partner-1", "loc-1", "SOME_OTHER_DOMAIN", "SOME_OTHER_SIGNAL", "xyz", "time_period_60", sqlDate("2022-02-24")),
                ("cust-4", "partner-1", "loc-1", "order_metrics", "total_orders", "25", "time_period_7", sqlDate("2022-02-24")),
                ("cust-5", "partner-2", "loc-1", "order_metrics", "total_orders", "17", "time_period_90", sqlDate("2022-02-24")),
                ("cust-4", "partner-1", "loc-1", "order_metrics", "total_orders", "123", "time_period_7", sqlDate("2022-02-25")),
                ("cust-5", "partner-2", "loc-1", "order_metrics", "total_orders", "234", "time_period_90", sqlDate("2022-02-25"))
            ).toDF("customer_360_id", "cxi_partner_id", "location_id", "signal_domain", "signal_name", "signal_value", "date_option", "signal_generation_date")
            actual_3.schema.fields.map(_.name) shouldEqual expected_3.schema.fields.map(_.name)
            actual_3.collect() should contain theSameElementsAs expected_3.collect()
        }

        dropTempTable(destTable)
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
        spark.sql(
            s"""
               |CREATE TABLE IF NOT EXISTS $tableName
               (
               |    `cxi_partner_id`         string not null,
               |    `location_id`            string not null,
               |    `date_option`            string not null,
               |    `customer_360_id`        string not null,
               |    `signal_domain`          string not null,
               |    `signal_name`            string not null,
               |    `signal_value`           string not null,
               |    `signal_generation_date` date   not null
               |) USING delta
               |PARTITIONED BY (signal_generation_date, signal_domain, signal_name, cxi_partner_id);
               |""".stripMargin)
    }

    def dropTempTable(tableName: String): Unit = {
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }

    private def sqlDate(date: String): java.sql.Date = {
        java.sql.Date.valueOf(java.time.LocalDate.parse(date))
    }
}
