package com.cxi.cdp.data_processing
package curated_zone.signal_framework.loyalty

import support.BaseSparkBatchJobTest

import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper}

class LoyaltyTypeSignalsJobTest extends BaseSparkBatchJobTest {

    import spark.implicits._

    test("Read orders and identities") {

        // given
        val customer360Table = "tempCustomer360Table"
        val customer360createDate = sqlDate("2022-02-24")
        val customer360Df = Seq(
            (
                "cust_id1",
                Map("combination-bin" -> Array("combination-bin_1", "combination-bin_2")),
                customer360createDate,
                customer360createDate,
                true
            ),
            ("cust_id2", Map("email" -> Array("test@domain.com")), customer360createDate, customer360createDate, true),
            // inactive customer, should be ignored
            (
                "cust_id3",
                Map("email" -> Array("test2@domain.com")),
                customer360createDate,
                customer360createDate,
                false
            ),
            (
                "cust_id4",
                Map("email" -> Array("customer4@domain.com")),
                customer360createDate,
                customer360createDate,
                true
            )
        ).toDF("customer_360_id", "identities", "create_date", "update_date", "active_flag")
        customer360Df.createOrReplaceTempView(customer360Table)

        // the rest of columns doesn't matter
        val orderSummaryTable = "tempOrderSummary"
        val cxiIdentitIdsType = ArrayType(
            StructType(
                Array(
                    StructField("identity_type", StringType),
                    StructField("cxi_identity_id", StringType)
                )
            )
        )
        val orderSummarySchema = StructType(
            Array(
                StructField("cxi_identity_ids", cxiIdentitIdsType),
                StructField("cxi_partner_id", StringType),
                StructField("location_id", StringType),
                StructField("ord_pay_total", DecimalType(9, 2)),
                StructField("ord_date", DateType),
                StructField("ord_id", StringType)
            )
        )

        val orderSummary = Seq(
            Row(
                Seq(Row("combination-bin", "combination-bin_1")),
                "partner1",
                "loc1",
                BigDecimal(10.0),
                sqlDate("2022-02-24"),
                "ord1"
            ),
            Row(
                Seq(Row("email", "test@test.com")),
                "partner1",
                "loc1",
                BigDecimal(20.0),
                sqlDate("2022-01-21"),
                "ord2"
            ),
            Row(Seq(Row("phone", "1234567")), "partner2", "loc3", BigDecimal(0.5), sqlDate("2022-01-15"), "ord3"),
            // order date is not defined, skip this row
            Row(Seq(Row("phone", "1234567")), "partner2", "loc3", BigDecimal(0.5), null, "ord4"),
            // identities are not defined, skip this row
            Row(Seq(), "partner2", "loc3", BigDecimal(0.5), null, "ord4")
        )
        import collection.JavaConverters._
        val orderSummaryDf = spark.createDataFrame(orderSummary.asJava, orderSummarySchema)
        orderSummaryDf.createOrReplaceTempView(orderSummaryTable)

        // when
        val (customer360identities, orders) = LoyaltyTypeSignalsJob.read(spark, customer360Table, orderSummaryTable)

        // then
        withClue("Customer 360 identities do not match") {
            val expected = Seq(
                ("cust_id1", "combination-bin:combination-bin_1"),
                ("cust_id1", "combination-bin:combination-bin_2"),
                ("cust_id2", "email:test@domain.com"),
                ("cust_id4", "email:customer4@domain.com")
            ).toDF("customer_360_id", "qualified_identity_id")

            customer360identities.schema.fields.map(_.name) shouldEqual expected.schema.fields.map(_.name)
            customer360identities.collect() should contain theSameElementsAs expected.collect()
        }
        withClue("Orders do not match") {
            val expected = Seq(
                Row(
                    "partner1",
                    "loc1",
                    BigDecimal(10.00),
                    sqlDate("2022-02-24"),
                    "ord1",
                    "combination-bin:combination-bin_1"
                ),
                Row("partner1", "loc1", BigDecimal(20.00), sqlDate("2022-01-21"), "ord2", "email:test@test.com"),
                Row("partner2", "loc3", BigDecimal(0.50), sqlDate("2022-01-15"), "ord3", "phone:1234567")
            )
            val expectedSchema = StructType(
                Array(
                    StructField("cxi_partner_id", StringType),
                    StructField("location_id", StringType),
                    StructField("ord_pay_total", DecimalType(9, 2)),
                    StructField("ord_date", DateType),
                    StructField("ord_id", StringType),
                    StructField("qualified_identity_id", StringType)
                )
            )
            import collection.JavaConverters._
            val expectedDf = spark.createDataFrame(expected.asJava, expectedSchema)
            orders.schema.fields.map(_.name) shouldEqual expectedDf.schema.fields.map(_.name)
            orders.collect() should contain theSameElementsAs expectedDf.collect()
        }
    }

    private def sqlDate(date: String): java.sql.Date = {
        java.sql.Date.valueOf(java.time.LocalDate.parse(date))
    }

}
