package com.cxi.cdp.data_processing
package curated_zone.mli

import support.BaseSparkBatchJobTest

import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.DataFrame
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper, equal}

class PartnerItemInsightsJobTest extends BaseSparkBatchJobTest {

    test("test transformOrderSummary") {
        // given
        import spark.implicits._

        val orderSummaryDf = List(
            (
                "39mqG77OEGjaf4HH2",
                sqlDate(2022, 5, 1),
                "cxi-usa-goldbowl",
                "L04HB3ZDYYD2M",
                "KYYDC4ULFYATEZ7XY",
                1,
                1,
                9.01,
                Array(
                    Map(
                        "identity_type" -> "phone",
                        "cxi_identity_id" -> "fa3998a6b3d42d65ebabb3fdf11f67ace7e2a419902db7673194ec8e80f4126f"
                    )
                )
            )
        ).toDF(
            "ord_id",
            "ord_date",
            "cxi_partner_id",
            "location_id",
            "item_id",
            "transaction_quantity",
            "item_quantity",
            "item_total",
            "cxi_identity_ids"
        )

        val locationDf = List(
            ("L04HB3ZDYYD2M", "cxi-usa-goldbowl", "TestLocationa71", "SouthEast", "GA", "Atlanta")
        ).toDF(
            "location_id",
            "cxi_partner_id",
            "location_nm",
            "region",
            "state_code",
            "city"
        )

        val itemDf = List(
            ("KYYDC4ULFYATEZ7XY", "cxi-usa-goldbowl", "Roll Bar-rito")
        ).toDF(
            "item_id",
            "cxi_partner_id",
            "item_nm"
        )

        val customer360Df = List(
            (
                "00051449-8e6c-4c10-b51f-afbfd5425887",
                Map("phone" -> Array("fa3998a6b3d42d65ebabb3fdf11f67ace7e2a419902db7673194ec8e80f4126f")),
                sqlDate(2022, 3, 17),
                sqlDate(2022, 7, 22),
                true
            ),
            (
                "000691ea-8314-4a8c-955e-49e8303261ab",
                Map("phone" -> Array("11d63c05d848647daebd254ec8394c25f1695b460b77c5097eb790093a850b8a")),
                sqlDate(2022, 7, 14),
                sqlDate(2022, 7, 22),
                true
            )
        ).toDF(
            "customer_360_id",
            "identities",
            "create_date",
            "update_date",
            "active_flag"
        )

        // when
        val actualOrderSummary =
            PartnerItemInsightsJob.transformOrderSummary(orderSummaryDf, locationDf, itemDf, customer360Df)

        // then
        val actualOrderFieldsReturned = actualOrderSummary.schema.fields.map(f => f.name)

        withClue("Actual fields returned:\n" + actualOrderSummary.schema.treeString) {
            actualOrderFieldsReturned should contain theSameElementsAs
                Seq(
                    "ord_id",
                    "ord_date",
                    "cxi_partner_id",
                    "region",
                    "state_code",
                    "city",
                    "location_id",
                    "location_nm",
                    "item_nm",
                    "item_quantity",
                    "item_total",
                    "customer_360_id"
                )
        }

        val actualTransformedOrderSummaryData = actualOrderSummary.collect()
        withClue("Transformed order summary do not match") {
            val expected = List(
                (
                    "39mqG77OEGjaf4HH2",
                    sqlDate(2022, 5, 1),
                    "cxi-usa-goldbowl",
                    "SouthEast",
                    "GA",
                    "Atlanta",
                    "L04HB3ZDYYD2M",
                    "TestLocationa71",
                    "Roll Bar-rito",
                    1,
                    9.01,
                    "00051449-8e6c-4c10-b51f-afbfd5425887"
                )
            ).toDF(
                "ord_id",
                "ord_date",
                "cxi_partner_id",
                "region",
                "state_code",
                "city",
                "location_id",
                "location_nm",
                "item_nm",
                "item_quantity",
                "item_total",
                "customer_360_id"
            ).collect()
            actualTransformedOrderSummaryData.length should equal(expected.length)
            actualTransformedOrderSummaryData should contain theSameElementsAs expected
        }
    }

    test("test computePartnerItemInsights") {
        // given
        import spark.implicits._

        val orderSummaryDf = List(
            (
                "39mqG77OEGjaf4HH2",
                sqlDate(2022, 5, 1),
                "cxi-usa-goldbowl",
                "SouthEast",
                "GA",
                "Atlanta",
                "L04HB3ZDYYD2M",
                "TestLocationa71",
                "Roll Bar-rito",
                1,
                9.01,
                "00051449-8e6c-4c10-b51f-afbfd5425887"
            )
        ).toDF(
            "ord_id",
            "ord_date",
            "cxi_partner_id",
            "region",
            "state_code",
            "city",
            "location_id",
            "location_nm",
            "item_nm",
            "item_quantity",
            "item_total",
            "customer_360_id"
        )

        // when
        val actualInsights = PartnerItemInsightsJob.computePartnerItemInsights(orderSummaryDf)

        // then
        val actualFieldsReturned = actualInsights.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actualInsights.schema.treeString) {
            actualFieldsReturned should contain theSameElementsAs
                Seq(
                    "ord_date",
                    "cxi_partner_id",
                    "region",
                    "state_code",
                    "city",
                    "location_id",
                    "location_nm",
                    "pos_item_nm",
                    "transaction_quantity",
                    "item_quantity",
                    "item_total",
                    "customer_360_ids"
                )
        }

        val actualInsightsDf = actualInsights.collect()
        withClue("compute partner item insights do not match") {
            val expected = List(
                (
                    sqlDate(2022, 5, 1),
                    "cxi-usa-goldbowl",
                    "SouthEast",
                    "GA",
                    "Atlanta",
                    "L04HB3ZDYYD2M",
                    "TestLocationa71",
                    "Roll Bar-rito",
                    1,
                    1,
                    9.01,
                    Array("00051449-8e6c-4c10-b51f-afbfd5425887")
                )
            ).toDF(
                "ord_date",
                "cxi_partner_id",
                "region",
                "state_code",
                "city",
                "location_id",
                "location_nm",
                "transaction_amount",
                "item_quantity",
                "item_category",
                "item_total",
                "customer_360_ids"
            ).collect()
            actualInsightsDf.length should equal(expected.length)
            actualInsightsDf should contain theSameElementsAs expected
        }
    }

    test("test getOrderDatesToProcess for empty change data") {
        import spark.implicits._
        val df = List.empty[java.sql.Date].toDF("ord_date")
        PartnerItemInsightsJob.getOrderDatesToProcess(df) shouldBe Set.empty
    }

    test("test getOrderDatesToProcess for non-empty change data with nulls") {
        import spark.implicits._
        val df = List(sqlDate(2021, 10, 1), null, sqlDate(2021, 10, 2)).toDF("ord_date")
        PartnerItemInsightsJob.getOrderDatesToProcess(df) shouldBe Set("2021-10-01", "2021-10-02")
    }

    def sqlDate(year: Int, month: Int, day: Int): java.sql.Date = {
        java.sql.Date.valueOf(java.time.LocalDate.of(year, java.time.Month.of(month), day))
    }

}
