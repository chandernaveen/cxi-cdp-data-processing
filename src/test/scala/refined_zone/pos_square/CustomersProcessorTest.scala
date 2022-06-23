package com.cxi.cdp.data_processing
package refined_zone.pos_square

import support.BaseSparkBatchJobTest

import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper, equal}

import java.sql.Timestamp.from
import java.time.LocalDateTime.of
import java.time.ZoneOffset.UTC

class CustomersProcessorTest extends BaseSparkBatchJobTest {

    test("test square partner customer read") {
        // given
        import spark.implicits._
        val customers = List(
            (
                s"""
                   {
                       "id":"L0P0DJ340FXF0",
                       "email_address":"ax3e49011927563cbf58161c436POA5fb2b09b4b12496fdc4e38d3e9784c3912",
                       "phone_number":"df2e49446821163cbf58168c436fec5fb2b09b4b12496fdc4e38d3e9784c2790",
                       "given_name":"John",
                       "family_name":"Doe",
                       "created_at":"2021-05-13T21:50:55Z",
                       "version": "2"
                    }
                   """,
                "customers",
                "2021-10-11"
            ),
            (
                s"""
                   {
                     "id": "L0P0DJ340FXF0"
                   }
                   """,
                "customers",
                "2021-10-10"
            ) // duplicate with diff date that gets filtered out
        ).toDF("record_value", "record_type", "feed_date")

        val tableName = "customers"
        customers.createOrReplaceTempView(tableName)

        // when
        val actual = CustomersProcessor.readCustomers(spark, "2021-10-11", tableName)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array(
                "customer_id",
                "email_address",
                "phone_number",
                "first_name",
                "last_name",
                "created_at",
                "version"
            )
        }
        val actualSquareCustomersData = actual.collect()
        withClue("POS Square refined customers data do not match") {
            val expected = List(
                (
                    "L0P0DJ340FXF0",
                    "ax3e49011927563cbf58161c436POA5fb2b09b4b12496fdc4e38d3e9784c3912",
                    "df2e49446821163cbf58168c436fec5fb2b09b4b12496fdc4e38d3e9784c2790",
                    "John",
                    "Doe",
                    "2021-05-13T21:50:55Z",
                    "2"
                )
            ).toDF("customer_id", "email_address", "phone_number", "first_name", "last_name", "created_at", "version")
                .collect()
            actualSquareCustomersData.length should equal(expected.length)
            actualSquareCustomersData should contain theSameElementsAs expected
        }
    }

    test("test square partner customer transformation") {
        // given
        import spark.implicits._
        val cxiPartnerId = "some-partner-id"
        val customers = List(
            ("L0P0DJ340FXF0", null, null, null, null, "2021-05-13T21:50:55.123Z", null),
            ("L0P0DJ340FXF0", null, null, null, null, "2021-05-13T21:50:55.123Z", null) // duplicate
        ).toDF("customer_id", "email_address", "phone_number", "first_name", "last_name", "created_at", "version")

        // when
        val actual = CustomersProcessor.transformCustomers(customers, cxiPartnerId)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned shouldEqual Array(
                "customer_id",
                "email_address",
                "phone_number",
                "first_name",
                "last_name",
                "created_at",
                "version",
                "cxi_partner_id"
            )
        }
        val actualSquareCustomersData = actual.collect()
        withClue("POS Square refined customers data do not match") {
            val expected = List(
                (
                    "L0P0DJ340FXF0",
                    null,
                    null,
                    null,
                    null,
                    from(of(2021, 5, 13, 21, 50, 55, 123000000).toInstant(UTC)),
                    null,
                    cxiPartnerId
                )
            ).toDF(
                "customer_id",
                "email_address",
                "phone_number",
                "first_name",
                "last_name",
                "created_at",
                "version",
                "cxi_partner_id"
            ).collect()
            actualSquareCustomersData.length should equal(expected.length)
            actualSquareCustomersData should contain theSameElementsAs expected
        }
    }

}
