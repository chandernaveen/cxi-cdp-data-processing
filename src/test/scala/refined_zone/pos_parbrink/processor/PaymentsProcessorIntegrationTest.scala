package com.cxi.cdp.data_processing
package refined_zone.pos_parbrink.processor

import refined_zone.pos_parbrink.model.{CardBrandType, PaymentStatusType}
import refined_zone.pos_parbrink.processor.PaymentsProcessorTest.PaymentRefined
import support.tags.RequiresDatabricksRemoteCluster
import support.BaseSparkBatchJobTest

import org.scalatest.BeforeAndAfterEach

@RequiresDatabricksRemoteCluster(reason = "Uses delta table so can not be executed locally")
class PaymentsProcessorIntegrationTest extends BaseSparkBatchJobTest with BeforeAndAfterEach {

    val destTable = generateUniqueTableName("integration_test_parbrink_payments_processor")

    test("write Parbrink payments") {
        import spark.implicits._

        // given
        val cxiPartnerId = "cxi-usa-partner-1"

        val payment_A = PaymentRefined(
            cxi_partner_id = cxiPartnerId,
            payment_id = "111_11",
            order_id = "111",
            location_id = "loc_id_1",
            status = PaymentStatusType.Active.value,
            name = "name_1",
            card_brand = CardBrandType.None.name,
            pan = "1234"
        )
        val payments_1 = Seq(payment_A).toDF()

        // when
        // write payments_1 first time
        PaymentsProcessor.writePayments(payments_1, cxiPartnerId, destTable)

        // then
        withClue("Saved payments do not match") {
            val actual = spark.table(destTable)
            assert("Column size not Equal", payments_1.columns.size, actual.columns.size)
            assertDataFrameEquals(payments_1, actual)
        }

        // when
        // write payments_1 one more time
        PaymentsProcessor.writePayments(payments_1, cxiPartnerId, destTable)

        // then
        // no duplicates
        withClue("Saved payments do not match") {
            val actual = spark.table(destTable)
            assert("Column size not Equal", payments_1.columns.size, actual.columns.size)
            assertDataFrameEquals(payments_1, actual)
        }

        // given
        val payment_A_modified = payment_A.copy(name = "name_1_modified")
        val payment_B = PaymentRefined(
            cxi_partner_id = cxiPartnerId,
            payment_id = "111_12",
            order_id = "111",
            location_id = "loc_id_1",
            status = PaymentStatusType.Deleted.value,
            name = "name_2",
            card_brand = null,
            pan = null
        )
        val payments_2 = Seq(payment_A_modified, payment_B).toDF()

        // when
        // write modified payment_A and new payment_B
        PaymentsProcessor.writePayments(payments_2, cxiPartnerId, destTable)

        // then
        // payment_A updated, payment_B added
        withClue("Saved payments do not match") {
            val actual = spark.table(destTable)
            assert("Column size not Equal", payments_2.columns.size, actual.columns.size)
            assertDataFrameEquals(payments_2, actual)
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
               |    `payment_id`     STRING,
               |    `order_id`       STRING,
               |    `location_id`    STRING,
               |    `status`         STRING,
               |    `name`           STRING,
               |    `card_brand`     STRING,
               |    `pan`            STRING,
               |    `bin`            STRING,
               |    `exp_month`      STRING,
               |    `exp_year`       STRING
               |) USING delta
               |PARTITIONED BY (cxi_partner_id);
               |""".stripMargin)
    }

    def dropTempTable(tableName: String): Unit = {
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }

}
