package com.cxi.cdp.data_processing
package curated_zone.mli

import support.BaseSparkBatchJobTest

import org.apache.spark.sql.DataFrame
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper, equal}

class MenuItemInsightJobTest {

    class MenuItemInsightJobTest extends BaseSparkBatchJobTest {

        test("test category read order_summary and join with item & location table")
        // given

        import spark.implicits._

        val locationDf = List(
            ("L04HB3ZDYYD2M", "cxi-usa-goldbowl", "1", "TestLocationa71", "SouthEast", "GA", " Atlanta"),
            ("L37Q5CQ90VZPN", "cxi-usa-goldbowl", "1", "Test Location de9", "SouthEast", "GA", "Atlanta"),
            ("L5P5MXATQR2FH", "cxi-usa-goldbowl", "1", "Test Location de9", "SouthEast", "GA", "Atlanta")
        ).toDF(
            "location_id",
            "cxi_partner_id",
            "location_type",
            "location_nm",
            "region",
            "state_code",
            "city"
        )
        val locationTable: String = "itemInsightsTable"
        locationDf.createOrReplaceGlobalTempView(locationTable)

        val orderSummaryDf = List(
            (
                "39mqG77OEGjaf4HH2",
                "2021-05-01",
                "cxi-usa-goldbowl",
                "L0RPRF21MQRZH",
                "JYYDC4ULFYATEZ7XY",
                "1",
                "9.01",
                "null"
            ),
            (
                "XHlPQnEKH74HHB7io",
                "2021-05-01",
                "cxi-usa-goldbowl",
                "L0RPRF21MQRZH",
                "YY7ZT6JP2YTZK2DXZ",
                "1",
                " 2.11",
                "null"
            ),
            (
                "39mqG77OEGjaf4HH2",
                "2021-05-01",
                "cxi-usa-goldbowl",
                "L0RPRF21MQRZH",
                "JYYDC4ULFYATEZ7XY",
                "1",
                "9.01",
                "null"
            )
        ).toDF(
            "ord_id",
            "ord_date",
            "cxi_partner_id",
            "location_id",
            "item_id",
            "item_quantity",
            "item_total",
            "cxi_identity_ids"
        )
        val orderSummaryTable: String = "itemInsightsTable"
        orderSummaryDf.createOrReplaceGlobalTempView(orderSummaryTable)
        val orderDates: Set[String] = Set("2022-12-10", "2022-12-12")

        val ItemDf = List(
            ("037d5042-20d9-4d8", "cxi-usa-toastbrea", "CATER Guac & Roll"),
            ("037d5042-20d9-4d8", "cxi-usa-toastbrea", "CATER Guac & Roll"),
            ("037d5042-20d9-4d8", "cxi-usa-toastbrea", "CATER Guac & Roll")
        ).toDF(
            "item_id",
            "cxi_partner_id",
            "item_nm"
        )

        val itemTable: String = "itemInsightsTable"
        ItemDf.createOrReplaceGlobalTempView(itemTable)

        // when
        val actual = MenuItemInsightJob.readOrderSummary(
            orderDates,
            s"global_temp.$orderSummaryTable",
            s"global_temp.$locationTable",
            s"global_temp.$itemTable"
        )(spark)

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned should contain theSameElementsAs
                Seq(
                    "location_id",
                    "cxi_partner_id",
                    "location_type",
                    "location_nm",
                    "region",
                    "state_code",
                    "city",
                    "ord_id",
                    "ord_date",
                    "item_id",
                    "item_quantity",
                    "item_total",
                    "cxi_identity_ids",
                    "item_nm"
                )

            val customer360Df = List(
                ("00051449-8e6c-4c1", "{phone -> [fa3998", "2022-03-17", "2022-07-22", "true"),
                ("00051449-8e6c-4c1", "{phone -> [fa3998", "2022-03-17", "2022-07-22", "true"),
                ("00051449-8e6c-4c1", "{phone -> [fa3998", "2022-03-17", "2022-07-22", "true")
            ).toDF(
                "customer_360_id",
                "identities",
                "create_date",
                "update_date",
                "active_flag"
            )
            val customer360Table: String = "itemInsightsTable"
            customer360Df.createOrReplaceGlobalTempView(customer360Table)
            val actual = MenuItemInsightJob.readCustomer360(
                s"global_temp.$customer360Table"
            )(spark)

            val actualFieldsReturned = actual.schema.fields.map(f => f.name)
            withClue("Actual fields returned:\n" + actual.schema.treeString) {
                actualFieldsReturned should contain theSameElementsAs
                    Seq(
                        "customer_360_id",
                        "ord_date",
                        "cxi_partner_id",
                        "region",
                        "state_code",
                        "city",
                        "location_id",
                        "location_nm",
                        "transaction_quantity",
                        "item_quantity",
                        "item_total",
                        "item_category",
                        "item_nm"
                    )

            }
            val actualPartnerItemInsightsData = actual.collect()
            withClue("Menu Item category insights Read data do not match") {
                val expected = List(
                    (
                        "2021-08-01",
                        "cxi-usa-goldbowl",
                        "6",
                        "Michigan Ave",
                        "MidWest",
                        "IL",
                        "Chicago",
                        "High Protein Brownie",
                        "2",
                        "2",
                        "4.46",
                        "81a91b4d-9cb1-4c4"
                    ),
                    (
                        "2021-08-01",
                        "cxi-usa-toastbreadless",
                        "2",
                        "Clybourn",
                        "SouthEast",
                        "GA",
                        "Atlanta",
                        "Fussy Hussy",
                        "1",
                        "3",
                        "50.09",
                        "589254fc-4830-4ec"
                    ),
                    (
                        "2021-08-01",
                        "cxi-usa-goldbowl",
                        "6",
                        "Michigan Ave",
                        "MidWest",
                        "IL",
                        "Chicago",
                        "High Protein Brownie",
                        "2",
                        "2",
                        "4.46",
                        "81a91b4d-9cb1-4c4"
                    )
                ).toDF(
                    "ord_date",
                    "cxi_partner_id",
                    "location_type",
                    "location_nm",
                    "region",
                    "state_code",
                    "city",
                    "item_nm",
                    "transaction_quantity",
                    "item_quantity",
                    "item_total"
                ).collect()
                actualPartnerItemInsightsData.length should equal(expected.length)
                actualPartnerItemInsightsData should contain theSameElementsAs expected
            }
        }

        test("test category aggregation insights computation") {
            // given
            import spark.implicits._

            val itemInsightsDataDf = List(
                (
                    "2021-08-01",
                    "cxi-usa-goldbowl",
                    "6",
                    "Michigan Ave",
                    "MidWest",
                    "IL",
                    "Chicago",
                    "High Protein Brownie",
                    "2",
                    "2",
                    "4.46",
                    "81a91b4d-9cb1-4c4"
                ),
                (
                    "2021-08-01",
                    "cxi-usa-toastbreadless",
                    "2",
                    "Clybourn",
                    "SouthEast",
                    "GA",
                    "Atlanta",
                    "Fussy Hussy",
                    "1",
                    "3",
                    "50.09",
                    "589254fc-4830-4ec"
                ),
                (
                    "2021-08-01",
                    "cxi-usa-goldbowl",
                    "6",
                    "Michigan Ave",
                    "MidWest",
                    "IL",
                    "Chicago",
                    "High Protein Brownie",
                    "2",
                    "2",
                    "4.46",
                    "81a91b4d-9cb1-4c4"
                )
            ).toDF(
                "ord_date",
                "cxi_partner_id",
                "region",
                "state_code",
                "city",
                "location_id",
                "location_nm",
                "transaction_quantity",
                "item_quantity",
                "item_total",
                "item_category",
                "item_nm"
            )

            // when
            val actual = MenuItemInsightJob.computePartnerItemInsights(itemInsightsDataDf)

            // then
            val actualFieldsReturned = actual.schema.fields.map(f => f.name)
            withClue("Actual fields returned:\n" + actual.schema.treeString) {
                actualFieldsReturned should contain theSameElementsAs
                    Seq(
                        "ord_date",
                        "cxi_partner_id",
                        "region",
                        "state_code",
                        "city",
                        "location_id",
                        "location_nm",
                        "transaction_quantity",
                        "item_quantity",
                        "item_category",
                        "item_total"
                    )
            }
            val actualItemInsightsData = actual.collect()
            withClue("Menu Item category insights aggr data do not match") {
                val expected = List(
                    (
                        "2021-08-01",
                        "cxi-usa-goldbowl",
                        "6",
                        "Michigan Ave",
                        "MidWest",
                        "IL",
                        "Chicago",
                        "High Protein Brownie",
                        "2",
                        "2",
                        "4.46",
                        "81a91b4d-9cb1-4c4"
                    ),
                    (
                        "2021-08-01",
                        "cxi-usa-toastbreadless",
                        "2",
                        "Clybourn",
                        "SouthEast",
                        "GA",
                        "Atlanta",
                        "Fussy Hussy",
                        "1",
                        "3",
                        "50.09",
                        "589254fc-4830-4ec"
                    ),
                    (
                        "2021-08-01",
                        "cxi-usa-goldbowl",
                        "6",
                        "Michigan Ave",
                        "MidWest",
                        "IL",
                        "Chicago",
                        "High Protein Brownie",
                        "2",
                        "2",
                        "4.46",
                        "81a91b4d-9cb1-4c4"
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
                    "item_total"
                ).collect()
                actualItemInsightsData.length should equal(expected.length)
                actualItemInsightsData should contain theSameElementsAs expected
            }
        }

        test("test getOrderDatesToProcess for empty change data") {
            import spark.implicits._
            val df = List.empty[java.sql.Date].toDF("ord_date")
            MenuItemInsightJob.getOrderDatesToProcess(df) shouldBe Set.empty
        }

        test("test getOrderDatesToProcess for non-empty change data with nulls") {
            import spark.implicits._
            val df = List(sqlDate(2021, 10, 1), null, sqlDate(2021, 10, 2)).toDF("ord_date")
            MenuItemInsightJob.getOrderDatesToProcess(df) shouldBe Set("2021-10-01", "2021-10-02")
        }

        private def sqlDate(year: Int, month: Int, day: Int): java.sql.Date = {
            java.sql.Date.valueOf(java.time.LocalDate.of(year, java.time.Month.of(month), day))
        }

    }
}
