package com.cxi.cdp.data_processing
package refined_zone.pos_omnivore

import refined_zone.hub.model.{OrderChannelType, OrderStateType}
import support.normalization.DateNormalization.parseToSqlDate
import support.normalization.TimestampNormalization.parseToTimestamp
import support.tags.RequiresDatabricksRemoteCluster
import support.BaseSparkBatchJobTest

import org.apache.spark.sql.{Encoders, Row}
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.Matchers._

@RequiresDatabricksRemoteCluster(reason = "Uses delta table so can not be executed locally")
class OrderSummaryProcessorIntegrationTest extends BaseSparkBatchJobTest with BeforeAndAfterEach {

    val destTable = generateUniqueTableName("integration_test_order_summary_omnivore")

    test("Write order summary to table") {

        val orderSummarySchema = DataTypes.createStructType(
            Array(
                StructField("ord_id", StringType),
                StructField("ord_desc", NullType),
                StructField("ord_total", DecimalType(9, 2)),
                StructField("ord_date", DateType, nullable = false),
                StructField("ord_timestamp", TimestampType, nullable = false),
                StructField("cxi_partner_id", StringType, nullable = false),
                StructField("location_id", StringType),
                StructField("ord_state_id", IntegerType, nullable = false),
                StructField("ord_type", StringType),
                StructField("ord_originate_channel_id", IntegerType),
                StructField("ord_target_channel_id", IntegerType),
                StructField("item_quantity", IntegerType),
                StructField("item_total", DecimalType(9, 2)),
                StructField("emp_id", StringType),
                StructField("discount_id", NullType),
                StructField("discount_amount", DecimalType(9, 2)),
                StructField("dsp_qty", NullType),
                StructField("dsp_ttl", NullType),
                StructField("guest_check_line_item_id", NullType),
                StructField("line_id", NullType),
                StructField("taxes_id", NullType),
                StructField("taxes_amount", DecimalType(9, 2)),
                StructField("item_id", StringType),
                StructField("item_price_id", NullType),
                StructField("reason_code_id", NullType),
                StructField("service_charge_id", NullType),
                StructField("service_charge_amount", DecimalType(9, 2)),
                StructField("total_taxes_amount", DecimalType(9, 2)),
                StructField("total_tip_amount", DecimalType(9, 2)),
                StructField("tender_ids", DataTypes.createArrayType(StringType)),
                StructField(
                    "cxi_identity_ids",
                    DataTypes.createArrayType(Encoders.product[PosIdentity].schema)
                ),
                StructField("ord_sub_total", DecimalType(9, 2)),
                StructField("ord_pay_total", DecimalType(9, 2)),
                StructField("feed_date", DateType)
            )
        )

        // given
        val orderSummaryData1 = Seq(
            Row(
                "ord-2",
                null,
                BigDecimal(2.15),
                parseToSqlDate("2022-07-07"),
                parseToTimestamp("2022-07-07T16:41:32Z"),
                "partner-1",
                "loc-id2",
                OrderStateType.Completed.code,
                null,
                OrderChannelType.Other.code,
                OrderChannelType.PhysicalDelivery.code,
                1,
                BigDecimal(1.99),
                "123",
                null,
                BigDecimal(0.00),
                null,
                null,
                null,
                null,
                null,
                BigDecimal(0.00),
                "item-id-1",
                null,
                null,
                null,
                BigDecimal(0.00),
                BigDecimal(0.16),
                BigDecimal(0.40),
                Array("123"),
                null,
                BigDecimal(1.99),
                BigDecimal(2.15),
                parseToSqlDate("2022-07-09")
            ),
            Row(
                "ord-1",
                null,
                BigDecimal(2.15),
                parseToSqlDate("2022-07-07"),
                parseToTimestamp("2022-07-07T16:41:32Z"),
                "partner-1",
                "loc-id1",
                OrderStateType.Completed.code,
                null,
                OrderChannelType.PhysicalLane.code,
                OrderChannelType.PhysicalLane.code,
                1,
                BigDecimal(1.99),
                "123",
                null,
                BigDecimal(0.00),
                null,
                null,
                null,
                null,
                null,
                BigDecimal(0.00),
                "item-id-1",
                null,
                null,
                null,
                BigDecimal(0.00),
                BigDecimal(0.16),
                BigDecimal(0.40),
                Array("123"),
                Array(PosIdentity("combination-card-slim", "card-slim-id")),
                BigDecimal(1.99),
                BigDecimal(2.15),
                parseToSqlDate("2022-07-09")
            )
        )

        import collection.JavaConverters._
        val orderSummary1 = spark.createDataFrame(orderSummaryData1.asJava, orderSummarySchema)

        // when
        // write order summary first time
        OrderSummaryProcessor.writeOrderSummary(orderSummary1, "partner-1", destTable)

        // then
        withClue("First time writing to table do not match") {
            val actual = spark
                .table(destTable)

            val expected = orderSummary1

            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }

        // when
        // writing same order summary one more time
        OrderSummaryProcessor.writeOrderSummary(orderSummary1, "partner-1", destTable)

        // then
        withClue("Rewriting initial set of rows - emulate job re-run/Duplicate checks") {
            val actual = spark
                .table(destTable)

            val expected = orderSummary1
            actual.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            actual.collect() should contain theSameElementsAs expected.collect()
        }

        // given
        val orderSummaryData2 = Seq(
            Row(
                "ord-2",
                null,
                BigDecimal(3.15),
                parseToSqlDate("2022-07-07"),
                parseToTimestamp("2022-07-07T16:41:32Z"),
                "partner-1",
                "loc-id2",
                OrderStateType.Completed.code,
                null,
                OrderChannelType.Other.code,
                OrderChannelType.PhysicalDelivery.code,
                1,
                BigDecimal(1.99),
                "123",
                null,
                BigDecimal(0.00),
                null,
                null,
                null,
                null,
                null,
                BigDecimal(0.00),
                "item-id-1",
                null,
                null,
                null,
                BigDecimal(0.00),
                BigDecimal(0.16),
                BigDecimal(0.40),
                Array("123"),
                null,
                BigDecimal(1.99),
                BigDecimal(2.15),
                parseToSqlDate("2022-07-09")
            ),
            Row(
                "ord-3",
                null,
                BigDecimal(1.15),
                parseToSqlDate("2022-07-07"),
                parseToTimestamp("2022-07-07T17:41:32Z"),
                "partner-1",
                "loc-id1",
                OrderStateType.Completed.code,
                null,
                OrderChannelType.PhysicalLane.code,
                OrderChannelType.PhysicalLane.code,
                1,
                BigDecimal(0.99),
                "123",
                null,
                BigDecimal(0.00),
                null,
                null,
                null,
                null,
                null,
                BigDecimal(0.00),
                "item-id-1",
                null,
                null,
                null,
                BigDecimal(0.00),
                BigDecimal(0.16),
                BigDecimal(0.40),
                Array("123"),
                Array(PosIdentity("combination-card-slim", "card-slim-id3")),
                BigDecimal(1.99),
                BigDecimal(2.15),
                parseToSqlDate("2022-07-09")
            )
        )
        val orderSummary2 = spark.createDataFrame(orderSummaryData2.asJava, orderSummarySchema)

        // when
        OrderSummaryProcessor.writeOrderSummary(orderSummary2, "partner-1", destTable)

        // then
        withClue("Rewriting existing row - duplicate checks/data update") {
            val actual = spark
                .table(destTable)

            val expectedData = Seq(
                Row(
                    "ord-1",
                    null,
                    BigDecimal(2.15),
                    parseToSqlDate("2022-07-07"),
                    parseToTimestamp("2022-07-07T16:41:32Z"),
                    "partner-1",
                    "loc-id1",
                    OrderStateType.Completed.code,
                    null,
                    OrderChannelType.PhysicalLane.code,
                    OrderChannelType.PhysicalLane.code,
                    1,
                    BigDecimal(1.99),
                    "123",
                    null,
                    BigDecimal(0.00),
                    null,
                    null,
                    null,
                    null,
                    null,
                    BigDecimal(0.00),
                    "item-id-1",
                    null,
                    null,
                    null,
                    BigDecimal(0.00),
                    BigDecimal(0.16),
                    BigDecimal(0.40),
                    Array("123"),
                    Array(PosIdentity("combination-card-slim", "card-slim-id")),
                    BigDecimal(1.99),
                    BigDecimal(2.15),
                    parseToSqlDate("2022-07-09")
                ),
                Row(
                    "ord-3",
                    null,
                    BigDecimal(1.15),
                    parseToSqlDate("2022-07-07"),
                    parseToTimestamp("2022-07-07T17:41:32Z"),
                    "partner-1",
                    "loc-id1",
                    OrderStateType.Completed.code,
                    null,
                    OrderChannelType.PhysicalLane.code,
                    OrderChannelType.PhysicalLane.code,
                    1,
                    BigDecimal(0.99),
                    "123",
                    null,
                    BigDecimal(0.00),
                    null,
                    null,
                    null,
                    null,
                    null,
                    BigDecimal(0.00),
                    "item-id-1",
                    null,
                    null,
                    null,
                    BigDecimal(0.00),
                    BigDecimal(0.16),
                    BigDecimal(0.40),
                    Array("123"),
                    Array(PosIdentity("combination-card-slim", "card-slim-id3")),
                    BigDecimal(1.99),
                    BigDecimal(2.15),
                    parseToSqlDate("2022-07-09")
                ),
                Row(
                    "ord-2",
                    null,
                    BigDecimal(3.15),
                    parseToSqlDate("2022-07-07"),
                    parseToTimestamp("2022-07-07T16:41:32Z"),
                    "partner-1",
                    "loc-id2",
                    OrderStateType.Completed.code,
                    null,
                    OrderChannelType.Other.code,
                    OrderChannelType.PhysicalDelivery.code,
                    1,
                    BigDecimal(1.99),
                    "123",
                    null,
                    BigDecimal(0.00),
                    null,
                    null,
                    null,
                    null,
                    null,
                    BigDecimal(0.00),
                    "item-id-1",
                    null,
                    null,
                    null,
                    BigDecimal(0.00),
                    BigDecimal(0.16),
                    BigDecimal(0.40),
                    Array("123"),
                    null,
                    BigDecimal(1.99),
                    BigDecimal(2.15),
                    parseToSqlDate("2022-07-09")
                )
            )

            import collection.JavaConverters._
            val expected = spark.createDataFrame(expectedData.asJava, orderSummarySchema)
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
            CREATE TABLE IF NOT EXISTS $tableName(
                `ord_id` STRING,
                `ord_desc` STRING,
                `ord_total` DECIMAL(9,2),
                `ord_date` DATE,
                `ord_timestamp` TIMESTAMP,
                `cxi_partner_id` STRING,
                `location_id` STRING,
                `ord_state_id` INT,
                `ord_type` STRING,
                `ord_originate_channel_id` INT,
                `ord_target_channel_id` INT,
                `item_quantity` INT,
                `item_total` DECIMAL(9,2),
                `emp_id` STRING,
                `discount_id` STRING,
                `discount_amount` DECIMAL(9,2),
                `dsp_qty` INT,
                `dsp_ttl` DECIMAL(9,2),
                `guest_check_line_item_id` STRING,
                `line_id` STRING,
                `taxes_id` STRING,
                `taxes_amount` DECIMAL(9,2),
                `item_id` STRING,
                `item_price_id` STRING,
                `reason_code_id` STRING,
                `service_charge_id` STRING,
                `service_charge_amount` DECIMAL(9,2),
                `total_taxes_amount` DECIMAL(9,2),
                `total_tip_amount` DECIMAL(9,2),
                `tender_ids` ARRAY<STRING>,
                `cxi_identity_ids` ARRAY<STRUCT<`identity_type`: STRING, `cxi_identity_id`: STRING>>,
                `ord_sub_total` DECIMAL(9,2),
                `ord_pay_total` DECIMAL(9,2),
                `feed_date` DATE
            )
            USING delta
            PARTITIONED BY (cxi_partner_id, ord_date)
            """.stripMargin)
    }

    def dropTempTable(tableName: String): Unit = {
        spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }

    case class PosIdentity(identity_type: String, cxi_identity_id: String)

}
