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
            ),
            (
                "XHlPQnEKH74HHB7ioKD0xayeV",
                sqlDate(2022, 5, 1),
                "cxi-usa-goldbowl",
                "L0P0DJ340FXF0",
                "YY7ZT6JP2YTZK2DXZNLUSW5S",
                1,
                2,
                2.11,
                Array(
                    Map(
                        "identity_type" -> "phone",
                        "cxi_identity_id" -> "ef9ff3c6122618676c42c40f009862264e0e947e2a5916354572aa0d97b43f11"
                    )
                )
            ),
            (
                "dMWbKnd3gbnqJNpiACKhEFzeV",
                sqlDate(2022, 5, 1),
                "cxi-usa-goldbowl",
                "L18BR1P2QB88W",
                "S2FOIIUKBCS47GLJBY5ET5WC",
                1,
                1,
                9.49,
                Array(
                    Map(
                        "identity_type" -> "phone",
                        "cxi_identity_id" -> "9e4b7049ee2d413583fb575d9536d43a90d0c45510d479be95f14df3766d89d6"
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
            ("L04HB3ZDYYD2M", "cxi-usa-goldbowl", "TestLocationa71", "SouthEast", "GA", "Atlanta"),
            ("L0P0DJ340FXF0", "cxi-usa-goldbowl", "DesertRidge", "NorthWest", "NV", "LasVegas"),
            ("L18BR1P2QB88W", "cxi-usa-goldbowl", "TestLocation6b", "SouthEast", "ID", "IdahoFalls")
        ).toDF(
            "location_id",
            "cxi_partner_id",
            "location_nm",
            "region",
            "state_code",
            "city"
        )

        val itemDf = List(
            ("KYYDC4ULFYATEZ7XY", "cxi-usa-goldbowl", "Roll Bar-rito"),
            ("YY7ZT6JP2YTZK2DXZNLUSW5S", "cxi-usa-goldbowl", "Super-bop"),
            ("S2FOIIUKBCS47GLJBY5ET5WC", "cxi-usa-goldbowl", "SideVeggie")
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
                "00097e86-e151-4f19-9472-7feb8f095632",
                Map("phone" -> Array("ef9ff3c6122618676c42c40f009862264e0e947e2a5916354572aa0d97b43f11")),
                sqlDate(2022, 7, 14),
                sqlDate(2022, 7, 22),
                true
            ),
            (
                "000b648e-6095-4ff2-88a6-1135d9626844",
                Map("phone" -> Array("9e4b7049ee2d413583fb575d9536d43a90d0c45510d479be95f14df3766d89d6")),
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
                ),
                (
                    "XHlPQnEKH74HHB7ioKD0xayeV",
                    sqlDate(2022, 5, 1),
                    "cxi-usa-goldbowl",
                    "NorthWest",
                    "NV",
                    "LasVegas",
                    "L0P0DJ340FXF0",
                    "DesertRidge",
                    "Super-bop",
                    2,
                    2.11,
                    "00097e86-e151-4f19-9472-7feb8f095632"
                ),
                (
                    "dMWbKnd3gbnqJNpiACKhEFzeV",
                    sqlDate(2022, 5, 1),
                    "cxi-usa-goldbowl",
                    "SouthEast",
                    "ID",
                    "IdahoFalls",
                    "L18BR1P2QB88W",
                    "TestLocation6b",
                    "SideVeggie",
                    1,
                    9.49,
                    "000b648e-6095-4ff2-88a6-1135d9626844"
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
            ),
            (
                "XHlPQnEKH74HHB7ioKD0xayeV",
                sqlDate(2022, 5, 1),
                "cxi-usa-goldbowl",
                "NorthWest",
                "NV",
                "LasVegas",
                "L0P0DJ340FXF0",
                "DesertRidge",
                "Super bop",
                2,
                2.11,
                "00097e86-e151-4f19-9472-7feb8f095632"
            ),
            (
                "dMWbKnd3gbnqJNpiACKhEFzeV",
                sqlDate(2022, 5, 1),
                "cxi-usa-goldbowl",
                "SouthEast",
                "ID",
                "Idaho Falls",
                "L18BR1P2QB88W",
                "TestLocation6b",
                "SideVeggie",
                1,
                9.49,
                "000b648e-6095-4ff2-88a6-1135d9626844"
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
                ),
                (
                    sqlDate(2022, 5, 1),
                    "cxi-usa-goldbowl",
                    "NorthWest",
                    "NV",
                    "LasVegas",
                    "L0P0DJ340FXF0",
                    "DesertRidge",
                    "Super bop",
                    1,
                    2,
                    2.11,
                    Array("00097e86-e151-4f19-9472-7feb8f095632")
                ),
                (
                    sqlDate(2022, 5, 1),
                    "cxi-usa-goldbowl",
                    "SouthEast",
                    "ID",
                    "Idaho Falls",
                    "L18BR1P2QB88W",
                    "TestLocation6b",
                    "SideVeggie",
                    1,
                    1,
                    9.49,
                    Array("000b648e-6095-4ff2-88a6-1135d9626844")
                )
            ).toDF(
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
