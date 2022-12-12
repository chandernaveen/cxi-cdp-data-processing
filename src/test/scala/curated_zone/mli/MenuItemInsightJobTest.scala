package com.cxi.cdp.data_processing
package curated_zone.mli

import support.BaseSparkBatchJobTest

import org.apache.spark.sql.DataFrame
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper, equal}

class MenuItemInsightJobTest extends BaseSparkBatchJobTest {

    test("test category read order_summary and join with item & location table") {
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
        val locationTable: String = "locationTable"
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
        val orderSummaryTable: String = "orderSummaryTable"
        orderSummaryDf.createOrReplaceGlobalTempView(orderSummaryTable)

        val orderDates: Set[String] = Set("2022-12-10", "2022-12-12")

        val ItemDf = List(
            ("009fd956-19e3-479a-941e-2f98e24b83da", "cxi-usa-toastbrea", "CATER Guac & Roll Bar-rito GF"),
            ("030ac764-f3fe-4c3f-8b30-d8084ab08ebd", "cxi-usa-toastbrea", "Basic Beach"),
            ("0032653d-f836-4e0d-87c8-2f368434a8da", "cxi-usa-toastbrea", "Hippie")
        ).toDF(
            "item_id",
            "cxi_partner_id",
            "item_nm"
        )

        val itemTable: String = "itemTable"
        ItemDf.createOrReplaceGlobalTempView(itemTable)

        // when
        val actualOrderSummary = MenuItemInsightJob.readOrderSummary(
            orderDates,
            s"global_temp.$orderSummaryTable",
            s"global_temp.$locationTable",
            s"global_temp.$itemTable"
        )(spark)

        // then
        val actualOrderFieldsReturned = actualOrderSummary.schema.fields.map(f => f.name)

        withClue("Actual fields returned:\n" + actualOrderSummary.schema.treeString) {
            actualOrderFieldsReturned should contain theSameElementsAs
                Seq(
                    "ord_id",
                    "cxi_partner_id",
                    "item_id",
                    "region",
                    "state",
                    "city",
                    "location_id",
                    "location_nm",
                    "ord_date",
                    "item_quantity",
                    "item_total",
                    "item_id",
                    "item_nm"
                )
        }

        val actualReadOrderSummaryData = actualOrderSummary.collect()
        withClue("Read Order Summary Do not match") {

            // #######for a DF of read

            val expected = List(
                (
                    "400000014629071478",
                    "cxi-usa-sluttyvegan",
                    "037d5042-20d9-4d82-a971-cc1075e17e4e",
                    "SouthEast",
                    "GA",
                    "Atlantao",
                    "3",
                    "Jonesboro",
                    "2021-10-01",
                    "1",
                    "14.04",
                    "400000005753879999",
                    "Sloppy Toppy"
                ),
                (
                    "400000014630082332",
                    "cxi-usa-sluttyvegan",
                    "037d5042-20d9-4d82-a971-cc1075e17e4e",
                    "SouthEast",
                    "GA",
                    "Atlantao",
                    "3",
                    "Jonesboro",
                    "2021-10-01",
                    "2",
                    "28.08 ",
                    "400000005753879999",
                    "Fussy Hussy"
                ),
                (
                    "400000014630082332",
                    "cxi-usa-sluttyvegan",
                    "037d5042-20d9-4d82-a971-cc1075e17e4e",
                    "SouthEast",
                    "GA",
                    "Atlantao",
                    "3",
                    "Jonesboro",
                    "2021-10-01",
                    "1",
                    "5.44",
                    "400000006428559450",
                    "One Night Stand"
                )
            ).toDF(
                "ord_id",
                "cxi_partner_id",
                "item_id",
                "region",
                "state",
                "city",
                "location_id",
                "location_nm",
                "ord_date",
                "item_quantity",
                "item_total",
                "item_id",
                "item_nm"
            ).collect()
            actualReadOrderSummaryData.length should equal(expected.length)
            actualReadOrderSummaryData should contain theSameElementsAs expected

        }

    }

    test("test ite, aggr read customer360 ") {

        import spark.implicits._

        val customer360Df = List(
            (
                "00051449-8e6c-4c10-b51f-afbfd5425887",
                "phone -> [fa3998a6b3d42d65ebabb3fdf11f67ace7e2a419902db7673194ec8e80f4126f",
                "2022-03-17",
                "2022-07-22",
                "true"
            ), /// fix the identities values
            (
                "000691ea-8314-4a8c-955e-49e8303261ab",
                "phone -> [11d63c05d848647daebd254ec8394c25f1695b460b77c5097eb790093a850b8a",
                "2022-07-14",
                "2022-07-22",
                "true"
            ),
            (
                "000b648e-6095-4ff2-88a6-1135d9626844",
                "phone -> [143a9f71224c5a1ff5db5fd071dc557ae781e3e5136d7bc1c85b2edf3eaa43df",
                "2022-03-17",
                "2022-07-22",
                "true"
            )
        ).toDF(
            "customer_360_id",
            "identities",
            "create_date",
            "update_date",
            "active_flag"
        )

        val customer360Table: String = "customer360Table"
        customer360Df.createOrReplaceGlobalTempView(customer360Table)

        val actualCustomer360 = MenuItemInsightJob.readCustomer360(
            s"global_temp.$customer360Table"
        )(spark)

        val actualCustomer360FieldsReturned = actualCustomer360.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actualCustomer360.schema.treeString) {
            actualCustomer360FieldsReturned should contain theSameElementsAs
                Seq( // fix the column list
                    "customer_360_id",
                    "qualified_identity"
                )

            val actualCustomer360Data = actualCustomer360.collect()
            withClue("Read Customer360 data do not match") {
                val expected = List( // fix the expected data
                    (
                        "0000d421-b522-4f6c-a00b-ec248f2261af",
                        "combination-card:fa53a1e253812f201e5b335226d4764c911bd12c5a3568f91af7019c7bd2ecfc"
                    ),
                    (
                        "00083af1-bcd0-4967-ac8b-f307de4ea146",
                        "combination-card:a6e863a51f64b62235bf0036dd64bad1dbceae2ee7415013f8a3b01bef2eb783"
                    ),
                    (
                        "000cfa78-6697-4f3f-b551-74cd56d628e6",
                        "phone:378b59b0484410a9f14d7448605858a5cad25ff67d21563460c6918316574c45 "
                    )
                ).toDF(
                    "customer_360_id",
                    "qualified_identity"
                ).collect()
                actualCustomer360Data.length should equal(expected.length)
                actualCustomer360Data should contain theSameElementsAs expected
            }

        }
    }
    test("test item aggregation insights computation") {
        // given
        import spark.implicits._

        // Create DF for orderSummary output of readOrdeerSummary

        val itemInsightsDataDf = List(
            (
                "400000014629071478",
                "cxi-usa-sluttyvegan",
                "037d5042-20d9-4d82-a971-cc1075e17e4e",
                "SouthEast",
                "GA",
                "Atlantao",
                "3",
                "Jonesboro",
                "2021-10-01",
                "1",
                "14.04",
                "400000005753879999",
                "Sloppy Toppy"
            ),
            (
                "400000014630082332",
                "cxi-usa-sluttyvegan",
                "037d5042-20d9-4d82-a971-cc1075e17e4e",
                "SouthEast",
                "GA",
                "Atlantao",
                "3",
                "Jonesboro",
                "2021-10-01",
                "2",
                "28.08 ",
                "400000005753879999",
                "Fussy Hussy"
            ),
            (
                "400000014630082332",
                "cxi-usa-sluttyvegan",
                "037d5042-20d9-4d82-a971-cc1075e17e4e",
                "SouthEast",
                "GA",
                "Atlantao",
                "3",
                "Jonesboro",
                "2021-10-01",
                "1",
                "5.44",
                "400000006428559450",
                "One Night Stand"
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

        // Create DF for customer360 output of readCustomer360
        val readCustomer360Df = List(
            (
                "0000d421-b522-4f6c-a00b-ec248f2261af",
                "combination-card:fa53a1e253812f201e5b335226d4764c911bd12c5a3568f91af7019c7bd2ecfc"
            ),
            (
                "00083af1-bcd0-4967-ac8b-f307de4ea146",
                "combination-card:a6e863a51f64b62235bf0036dd64bad1dbceae2ee7415013f8a3b01bef2eb783"
            ),
            (
                "000cfa78-6697-4f3f-b551-74cd56d628e6",
                "phone:378b59b0484410a9f14d7448605858a5cad25ff67d21563460c6918316574c45 "
            )
        ).toDF(
            "customer_360_id",
            "qualified_identity"
        )

        // when
        val actual = MenuItemInsightJob.computePartnerItemInsights(
            itemInsightsDataDf,
            readCustomer360Df
        ) /// ###Include Customer360 DF

        // then
        val actualFieldsReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualFieldsReturned should contain theSameElementsAs
                Seq( /// validate proper field names
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
        val actualItemInsightsData = actual.collect()
        withClue("Menu Item aggr insights data do not match") {
            val expected = List( // validate proper data items
                (
                    "400000014629071478",
                    "cxi-usa-sluttyvegan",
                    "037d5042-20d9-4d82-a971-cc1075e17e4e",
                    "SouthEast",
                    "GA",
                    "Atlantao",
                    "3",
                    "Jonesboro",
                    "2021-10-01",
                    "1",
                    "14.04",
                    "400000005753879999",
                    "Sloppy Toppy"
                ),
                (
                    "400000014630082332",
                    "cxi-usa-sluttyvegan",
                    "037d5042-20d9-4d82-a971-cc1075e17e4e",
                    "SouthEast",
                    "GA",
                    "Atlantao",
                    "3",
                    "Jonesboro",
                    "2021-10-01",
                    "2",
                    "28.08 ",
                    "400000005753879999",
                    "Fussy Hussy"
                ),
                (
                    "400000014630082332",
                    "cxi-usa-sluttyvegan",
                    "037d5042-20d9-4d82-a971-cc1075e17e4e",
                    "SouthEast",
                    "GA",
                    "Atlantao",
                    "3",
                    "Jonesboro",
                    "2021-10-01",
                    "1",
                    "5.44",
                    "400000006428559450",
                    "One Night Stand"
                )
            ).toDF( // validate proper dataframe columns
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
        val actualCustomer360sReturned = actual.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actual.schema.treeString) {
            actualCustomer360sReturned should contain theSameElementsAs
                Seq( // fix the column list
                    "customer_360_id",
                    "qualified_identity"
                )
            val actualCustomer360sreturned = actual.collect()
            withClue("Read Customer360 data do not match") {
                val expected = List( // fix the expected data
                    (
                        "0000d421-b522-4f6c-a00b-ec248f2261af",
                        "combination-card:fa53a1e253812f201e5b335226d4764c911bd12c5a3568f91af7019c7bd2ecfc"
                    ),
                    (
                        "00083af1-bcd0-4967-ac8b-f307de4ea146",
                        "combination-card:a6e863a51f64b62235bf0036dd64bad1dbceae2ee7415013f8a3b01bef2eb783"
                    ),
                    (
                        "000cfa78-6697-4f3f-b551-74cd56d628e6",
                        "phone:378b59b0484410a9f14d7448605858a5cad25ff67d21563460c6918316574c45 "
                    )
                ).toDF(
                    "customer_360_id",
                    "qualified_identity"
                ).collect()
                actualCustomer360sreturned.length should equal(expected.length)
                actualCustomer360sreturned should contain theSameElementsAs expected

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

        def sqlDate(year: Int, month: Int, day: Int): java.sql.Date = {
            java.sql.Date.valueOf(java.time.LocalDate.of(year, java.time.Month.of(month), day))
        }

    }
}
