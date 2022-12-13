package com.cxi.cdp.data_processing
package curated_zone.mli

import support.BaseSparkBatchJobTest

import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.DataFrame
import org.scalatest.Matchers.{contain, convertToAnyShouldWrapper, equal}

class MenuItemInsightJobTest extends BaseSparkBatchJobTest {

    test("test category read order_summary and join with item & location table") {
        // given
        import spark.implicits._

        val locationDf = List(
            ("L04HB3ZDYYD2M", "cxi-usa-goldbowl", "1", "TestLocationa71", "SouthEast", "GA", "Atlanta")
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

        val cxi_identity_Struct = new StructType()
            .add("identity_type", StringType)
            .add("cxi_partner_id", StringType)
        val cxi_identity_array = new ArrayType(cxi_identity_Struct, true)

        val orderSummaryDf = List(
            (
                "39mqG77OEGjaf4HH2",
                "2022-05-01",
                "cxi-usa-goldbowl",
                "L04HB3ZDYYD2M",
                "KYYDC4ULFYATEZ7XY",
                "1",
                "9.01",
                /*  """[
                    {"identity_type": "combination-card",
                    "cxi_identity_id": "b5f7cd6029f0d194bc5cb572ae03096910cda2af62502c559beeeb597fb55e72"
                    }
                    ]
""",*/
                Array(
                    Map(
                        "identity_type" -> "combination-card",
                        "cxi_identity_id" -> "b5f7cd6029f0d194bc5cb572ae03096910cda2af62502c559beeeb597fb55e72"
                    )
                )
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

        val orderDates: Set[String] = Set("2022-05-01", "2022-05-02")

        val ItemDf = List(
            ("KYYDC4ULFYATEZ7XY", "cxi-usa-goldbowl", "Roll Bar-rito")
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

        print(actualOrderSummary.collect().mkString)

        // then
        val actualOrderFieldsReturned = actualOrderSummary.schema.fields.map(f => f.name)

        withClue("Actual fields returned:\n" + actualOrderSummary.schema.treeString) {
            actualOrderFieldsReturned should contain theSameElementsAs
                Seq(
                    "ord_id",
                    "cxi_partner_id",
                    "region",
                    "state_code",
                    "city",
                    "location_id",
                    "location_nm",
                    "ord_date",
                    "item_quantity",
                    "item_total",
                    "pos_item_nm",
                    "qualified_identity"
                )
        }

        val actualReadOrderSummaryData = actualOrderSummary.collect()
        withClue("Read Order Summary Do not match") {

            val expected = List(
                (
                    "39mqG77OEGjaf4HH2",
                    "cxi-usa-goldbowl",
                    "SouthEast",
                    "GA",
                    "Atlanta",
                    "L04HB3ZDYYD2M",
                    "TestLocationa71",
                    "2022-05-01",
                    "1",
                    "9.01",
                    "Roll Bar-rito",
                    "combination-card:b5f7cd6029f0d194bc5cb572ae03096910cda2af62502c559beeeb597fb55e72"
                )
            ).toDF(
                "ord_id",
                "cxi_partner_id",
                "region",
                "state_code",
                "city",
                "location_id",
                "location_nm",
                "ord_date",
                "item_quantity",
                "item_total",
                "pos_item_nm",
                "qualified_identity"
            ).collect()
            actualReadOrderSummaryData.length should equal(expected.length)
            actualReadOrderSummaryData should contain theSameElementsAs expected

        }

    }

    test("test item, aggr read customer360 ") {

        import spark.implicits._

        val customer360Df = List(
            (
                "00051449-8e6c-4c10-b51f-afbfd5425887",
                Map("phone" -> Array("fa3998a6b3d42d65ebabb3fdf11f67ace7e2a419902db7673194ec8e80f4126f")),
                "2022-03-17",
                "2022-07-22",
                "true"
            ),
            (
                "000691ea-8314-4a8c-955e-49e8303261ab",
                Map("phone" -> Array("11d63c05d848647daebd254ec8394c25f1695b460b77c5097eb790093a850b8a")),
                "2022-07-14",
                "2022-07-22",
                "false"
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
                        "00051449-8e6c-4c10-b51f-afbfd5425887",
                        "phone:fa3998a6b3d42d65ebabb3fdf11f67ace7e2a419902db7673194ec8e80f4126f"
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

        val OrderSummaryDataDf = List(
            (
                "39mqG77OEGjaf4HH2",
                "cxi-usa-goldbowl",
                "SouthEast",
                "GA",
                "Atlanta",
                "L04HB3ZDYYD2M",
                "TestLocationa71",
                "2022-05-01",
                "1",
                "9.01",
                "Roll Bar-rito",
                "phone:fa3998a6b3d42d65ebabb3fdf11f67ace7e2a419902db7673194ec8e80f4126f"
            )
        ).toDF(
            "ord_id",
            "cxi_partner_id",
            "region",
            "state_code",
            "city",
            "location_id",
            "location_nm",
            "ord_date",
            "item_quantity",
            "item_total",
            "pos_item_nm",
            "qualified_identity"
        )

        val readCustomer360Df = List(
            (
                "00051449-8e6c-4c10-b51f-afbfd5425887",
                "phone:fa3998a6b3d42d65ebabb3fdf11f67ace7e2a419902db7673194ec8e80f4126f"
            )
        ).toDF(
            "customer_360_id",
            "qualified_identity"
        )

        // when
        val actualInsights = MenuItemInsightJob.computePartnerItemInsights(
            OrderSummaryDataDf,
            readCustomer360Df
        )

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
        withClue("Menu Item aggr insights data do not match") {
            val expected = List( // validate proper data items
                (
                    "2022-05-01",
                    "cxi-usa-goldbowl",
                    "SouthEast",
                    "GA",
                    "Atlanta",
                    "L04HB3ZDYYD2M",
                    "TestLocationa71",
                    "Roll Bar-rito",
                    1,
                    1.0,
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
