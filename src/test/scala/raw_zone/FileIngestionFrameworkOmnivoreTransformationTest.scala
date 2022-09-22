package com.cxi.cdp.data_processing
package raw_zone

import com.cxi.cdp.data_processing.model.pos_omnivore.{OmnivoreLandingZoneModel, OmnivoreRawZoneModel}
import com.cxi.cdp.data_processing.model.pos_omnivore.OmnivoreLandingZoneModel.{
    Address,
    CategoryEmbedded,
    Embedded,
    Items,
    Location,
    MenuCategories,
    MenuItem,
    MenuItemEmbedded,
    OrderType,
    PaymentEmbedded,
    Payments,
    PriceLevels,
    TenderType,
    TicketEmbedded,
    TicketsRootInterface,
    Totals
}
import com.cxi.cdp.data_processing.raw_zone.file_ingestion_framework.FileIngestionFrameworkTransformations
import support.BaseSparkBatchJobTest

import org.scalatest.Matchers

class FileIngestionFrameworkOmnivoreTransformationTest extends BaseSparkBatchJobTest with Matchers {

    test("test Omnivore transformation with customers data") {
        // Location
        val location1 = Location(
            address = Address(city = null, country = null, state = null, street1 = null, street2 = null, zip = null),
            display_name = "MicrosTest",
            name = "cjgjjkpi",
            owner = "CARDFREE",
            status = "online"
        )

        // OrderType
        val orderType1 = OrderType(available = true, id = "1", name = "Dine In", pos_id = "1")

        // Tickets
        val priceLevelsData = PriceLevels(
            barcodes = null,
            id = "1",
            name = "REG",
            price_per_unit = 199
        )

        val menuCategoriesData = MenuCategories(
            id = "f1000",
            level = 0,
            name = "APPETIZERS",
            pos_id = "1000"
        )
        val categoryEmbedded = CategoryEmbedded(
            menu_categories = Seq(menuCategoriesData),
            price_levels = Seq(priceLevelsData)
        )
        val menuItem_data = MenuItem(
            _embedded = categoryEmbedded,
            barcodes = null,
            id = "1001",
            in_stock = true,
            name = "Coffee",
            open = false,
            open_name = false,
            pos_id = "1001",
            price_per_unit = 199
        )
        val menuItemEmbeddedData = MenuItemEmbedded(menu_item = menuItem_data)

        val itemsData = Items(
            _embedded = menuItemEmbeddedData,
            id = "63323-1",
            included_tax = 0,
            name = "Coffee",
            price = 199,
            quantity = 1,
            seat = 1,
            sent = true,
            sent_at = 1656519489,
            split = 1
        )

        val tender_Type_Data = TenderType(
            allows_tips = true,
            id = "123",
            name = "3rd Party",
            pos_id = "107"
        )

        val paymentEmbedded = PaymentEmbedded(
            tender_type = tender_Type_Data
        )

        val paymentsData = Payments(
            _embedded = paymentEmbedded,
            amount = 1478,
            id = "26",
            tip = 299,
            _type = "other"
        )
        val ticketEmbedded = TicketEmbedded(
            items = Seq(itemsData),
            payments = Seq(paymentsData)
        )

        val ticketsRootInterface = TicketsRootInterface(
            _embedded = ticketEmbedded,
            auto_send = true,
            closed_at = 1656519489,
            // correlation = null,
            fire_date = "2022-06-29",
            fire_time = "09:16:09",
            guest_count = 1,
            id = "14782",
            name = "Iryna t.",
            open = false,
            opened_at = 1656519489,
            ticket_number = 1298,
            totals = Totals(
                discounts = 0,
                due = 0,
                exclusive_tax = 108,
                inclusive_tax = 0,
                items = 1370,
                other_charges = 0,
                paid = 1478,
                service_charges = 0,
                sub_total = 1370,
                tax = 108,
                tips = 299,
                total = 1478
            ),
            _void = false
        )
        val _embedded1 = Embedded(
            locations = Seq(location1),
            order_types = null,
            tickets = null
        )

        val _embedded2 = Embedded(
            locations = null,
            order_types = Seq(orderType1),
            tickets = null
        )
        val _embedded3 = Embedded(
            locations = null,
            order_types = null,
            tickets = Seq(ticketsRootInterface)
        )

        val landingDataLocation = OmnivoreLandingZoneModel(
            feed_date = "2021-09-03",
            cxi_id = "fc4ecb01-4d6b-4e99-97a0-e3cb218d4fb0",
            file_name = "check.json",
            _embedded = _embedded1
        )
        val landingDataOrderType = OmnivoreLandingZoneModel(
            feed_date = "2021-09-04",
            cxi_id = "fc4ecb01-4d6b-4e99-97a0-e3cb218d4fb0",
            file_name = "check.json",
            _embedded = _embedded2
        )

        val landingDataTicket = OmnivoreLandingZoneModel(
            feed_date = "2021-09-05",
            cxi_id = "fc4ecb01-4d6b-4e99-97a0-e3cb218d4fb0",
            file_name = "check.json",
            _embedded = _embedded3
        )

        // landing Data Location
        val landingLocation = spark.createDataFrame(List(landingDataLocation))

        // when
        val actualLanding = FileIngestionFrameworkTransformations.transformOmnivore(landingLocation)

        // then
        val actualFieldsReturned1 = actualLanding.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actualLanding.schema.treeString) {
            actualFieldsReturned1 should contain theSameElementsAs Seq(
                "record_type",
                "record_value",
                "feed_date",
                "cxi_id",
                "file_name"
            )
        }

        // landing Data OrderType
        val landingOrderType = spark.createDataFrame(List(landingDataOrderType))

        // when
        val actualOrderType = FileIngestionFrameworkTransformations.transformOmnivore(landingOrderType)

        // then
        val actualFieldsReturned2 = actualOrderType.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actualOrderType.schema.treeString) {
            actualFieldsReturned2 should contain theSameElementsAs Seq(
                "record_type",
                "record_value",
                "feed_date",
                "cxi_id",
                "file_name"
            )
        }

        // Landing Data Ticket
        val landingTicket = spark.createDataFrame(List(landingDataTicket))

        // when
        val actualTicket = FileIngestionFrameworkTransformations.transformOmnivore(landingTicket)

        // then
        val actualFieldsReturned3 = actualTicket.schema.fields.map(f => f.name)
        withClue("Actual fields returned:\n" + actualTicket.schema.treeString) {
            actualFieldsReturned3 should contain theSameElementsAs Seq(
                "record_type",
                "record_value",
                "feed_date",
                "cxi_id",
                "file_name"
            )
        }

        import spark.implicits._
        // Raw Data Locations
        val actualRawData1 = actualLanding.as[OmnivoreRawZoneModel].collectAsList()
        withClue(s"""Customer check data does not match:
               | $actualRawData1
               |""".stripMargin) {
            val expected = List(
                OmnivoreRawZoneModel(
                    record_type = "locations",
                    record_value =
                        """{"address":{},"display_name":"MicrosTest","name":"cjgjjkpi","owner":"CARDFREE","status":"online"}""",
                    feed_date = "2021-09-03",
                    cxi_id = "fc4ecb01-4d6b-4e99-97a0-e3cb218d4fb0",
                    file_name = "check.json"
                )
            )
            actualRawData1 should contain theSameElementsAs expected
        }
        val actualRawData2 = actualOrderType.as[OmnivoreRawZoneModel].collectAsList()
        withClue(s"""Customer check data does not match:
               | $actualRawData2
               |""".stripMargin) {
            val expected = List(
                OmnivoreRawZoneModel(
                    record_type = "order_types",
                    record_value = """{"available":true,"id":"1","name":"Dine In","pos_id":"1"}""",
                    feed_date = "2021-09-04",
                    cxi_id = "fc4ecb01-4d6b-4e99-97a0-e3cb218d4fb0",
                    file_name = "check.json"
                )
            )
            actualRawData2 should contain theSameElementsAs expected
        }

        // Raw Data Ticket
        val actualRawData3 = actualTicket.as[OmnivoreRawZoneModel].collectAsList()
        withClue(s"""Customer check

               | $actualRawData3
               |""".stripMargin) {
            val expected = List(
                OmnivoreRawZoneModel(
                    record_type = "tickets",
                    record_value =
                        """{"_embedded":{"items":[{"_embedded":{"menu_item":{"_embedded":{"menu_categories":[{"id":"f1000","level":0,"name":"APPETIZERS","pos_id":"1000"}],"price_levels":[{"id":"1","name":"REG","price_per_unit":199}]},"id":"1001","in_stock":true,"name":"Coffee","open":false,"open_name":false,"pos_id":"1001","price_per_unit":199}},"id":"63323-1","included_tax":0,"name":"Coffee","price":199,"quantity":1,"seat":1,"sent":true,"sent_at":1656519489,"split":1}],"payments":[{"_embedded":{"tender_type":{"allows_tips":true,"id":"123","name":"3rd Party","pos_id":"107"}},"amount":1478,"id":"26","tip":299,"_type":"other"}]},"auto_send":true,"closed_at":1656519489,"fire_date":"2022-06-29","fire_time":"09:16:09","guest_count":1,"id":"14782","name":"Iryna t.","open":false,"opened_at":1656519489,"ticket_number":1298,"totals":{"discounts":0,"due":0,"exclusive_tax":108,"inclusive_tax":0,"items":1370,"other_charges":0,"paid":1478,"service_charges":0,"sub_total":1370,"tax":108,"tips":299,"total":1478},"_void":false}""",
                    feed_date = "2021-09-05",
                    cxi_id = "fc4ecb01-4d6b-4e99-97a0-e3cb218d4fb0",
                    file_name = "check.json"
                )
            )
            actualRawData3 should contain theSameElementsAs expected
        }
    }
}
