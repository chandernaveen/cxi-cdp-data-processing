package com.cxi.cdp.data_processing
package refined_zone.pos_simphony

import support.utils.ContractUtils
import support.SparkSessionFactory

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lit, max, when}

import java.nio.file.Paths

object RawRefinedOracleSimphonyPartnerJob {
    val logger = Logger.getLogger(this.getClass.getName)

    def main(args: Array[String]): Unit = {
        logger.info(s"""Received following args: ${args.mkString(",")}""")

        val contractPath = args(0)
        val date = args(1)
        val cxiPartnerId = args(2)
        val spark = SparkSessionFactory.getSparkSession()
        run(spark, contractPath, date, cxiPartnerId)
    }

    def run(spark: SparkSession, contractPath: String, date: String, cxiPartnerId: String): Unit = {
        val contract: ContractUtils = new ContractUtils(Paths.get(contractPath))

        val srcDbName = contract.prop[String]("raw.src_db")
        val srcTable = contract.prop[String]("raw.src_table")
        val destDbName = contract.prop[String]("refined.dest_db")

        LocationsProcessor.process(spark, contract, date, cxiPartnerId, srcDbName, srcTable, destDbName)
        MenuItemsProcessor.process(spark, contract, date, cxiPartnerId, srcDbName, srcTable, destDbName)
        ItemPricesProcessor.process(spark, contract, date, cxiPartnerId, srcDbName, srcTable, destDbName)
        CategoriesProcessor.process(spark, contract, date, cxiPartnerId, srcDbName, srcTable, destDbName)
        OrderTaxesProcessor.process(spark, contract, date, cxiPartnerId, srcDbName, srcTable, destDbName)
        OrderDiscountsProcessor.process(spark, contract, date, cxiPartnerId, srcDbName, srcTable, destDbName)
        OrderTenderTypesProcessor.process(spark, contract, date, cxiPartnerId, srcDbName, srcTable, destDbName)
        OrderServiceChargesProcessor.process(spark, contract, date, cxiPartnerId, srcDbName, srcTable, destDbName)
        OrderSummaryProcessor.process(spark, contract, date, cxiPartnerId, srcDbName, srcTable, destDbName)
    }
}

object LocationsProcessor {
    def process(
        spark: SparkSession,
        contract: ContractUtils,
        date: String,
        cxiPartnerId: String,
        srcDbName: String,
        srcTable: String,
        destDbName: String
    ): Unit = {

        val locationConfig: Map[String, Any] = contract.prop[Map[String, Any]]("refined.location")
        val locationTable = locationConfig("table").asInstanceOf[String]

        val locations = readLocations(spark, date, srcDbName, srcTable)

        val processedLocations = transformLocations(locations, cxiPartnerId)

        writeLocation(processedLocations, cxiPartnerId, s"$destDbName.$locationTable")
    }

    def readLocations(spark: SparkSession, date: String, dbName: String, table: String): DataFrame = {
        spark.sql(s"""
               |SELECT
               |get_json_object(record_value, "$$.locRef") as location_id,
               |get_json_object(record_value, "$$.active") as active_flg,
               |get_json_object(record_value, "$$.name") as location_nm
               |FROM $dbName.$table
               |WHERE record_type = "locations" AND feed_date = "$date"
               |""".stripMargin)
    }

    def transformLocations(locations: DataFrame, cxiPartnerId: String): DataFrame = {
        locations
            .withColumn("cxi_partner_id", lit(cxiPartnerId))
            .withColumn("active_flg", when(col("active_flg") === "true", 1).otherwise(0))
            .withColumn("location_type", lit(null))
            .withColumn("address_1", lit(null))
            .withColumn("address_2", lit(null))
            .withColumn("city", lit(null))
            .withColumn("state", lit(null))
            .withColumn("lat", lit(null))
            .withColumn("long", lit(null))
            .withColumn("phone", lit(null))
            .withColumn("fax", lit(null))
            .withColumn("country_code", lit(null))
            .withColumn("parent_location_id", lit(null))
            .withColumn("currency", lit(null))
            .withColumn("open_dt", lit(null))
            .withColumn("timezone", lit(null))
            .withColumn("region", lit(null))
            .withColumn("zip_code", lit(null))
            .withColumn("extended_attr", lit(null))
            .dropDuplicates("cxi_partner_id", "location_id")
    }

    def writeLocation(df: DataFrame, cxiPartnerId: String, destTable: String): Unit = {
        val srcTable = "newLocations"

        df.createOrReplaceTempView(srcTable)
        df.sqlContext.sql(s"""
               |MERGE INTO $destTable
               |USING $srcTable
               |ON $destTable.cxi_partner_id = "$cxiPartnerId" AND $destTable.location_id = $srcTable.location_id
               |WHEN MATCHED
               |  THEN UPDATE SET *
               |WHEN NOT MATCHED
               |  THEN INSERT *
               |""".stripMargin)
    }
}

object MenuItemsProcessor {
    def process(
        spark: SparkSession,
        contract: ContractUtils,
        date: String,
        cxiPartnerId: String,
        srcDbName: String,
        srcTable: String,
        destDbName: String
    ): Unit = {

        val menuItemConfig: Map[String, Any] = contract.prop[Map[String, Any]]("refined.item")
        val menuItemTable = menuItemConfig("table").asInstanceOf[String]

        val menuItems = readMenuItems(spark, date, srcDbName, srcTable)

        val processedMenuItems = transformMenuItems(menuItems, cxiPartnerId)

        writeMenuItems(processedMenuItems, cxiPartnerId, s"$destDbName.$menuItemTable")
    }

    def readMenuItems(spark: SparkSession, date: String, dbName: String, table: String): DataFrame = {
        spark.sql(s"""
               |SELECT
               |get_json_object(record_value, "$$.num") as item_id,
               |get_json_object(record_value, "$$.name") as item_nm,
               |get_json_object(record_value, "$$.majGrpNum") as main_category_id,
               |get_json_object(record_value, "$$.majGrpName") as main_category_name,
               |get_json_object(record_value, "$$.famGrpNum") as sub_category_id
               |FROM $dbName.$table
               |WHERE record_type = "menuItems" AND feed_date = "$date"
               |""".stripMargin)
    }

    def transformMenuItems(menuItems: DataFrame, cxiPartnerId: String): DataFrame = {
        menuItems
            .withColumn("cxi_partner_id", lit(cxiPartnerId))
            .withColumn("item_plu", lit(null))
            .withColumn("item_barcode", lit(null))
            .dropDuplicates("cxi_partner_id", "item_id")
    }

    def writeMenuItems(df: DataFrame, cxiPartnerId: String, destTable: String): Unit = {
        val srcTable = "newMenuItems"

        df.createOrReplaceTempView(srcTable)
        df.sqlContext.sql(s"""
               |MERGE INTO $destTable
               |USING $srcTable
               |ON $destTable.cxi_partner_id = "$cxiPartnerId" AND $destTable.item_id = $srcTable.item_id
               |WHEN MATCHED
               |  THEN UPDATE SET *
               |WHEN NOT MATCHED
               |  THEN INSERT *
               |""".stripMargin)
    }
}

object ItemPricesProcessor {
    def process(
        spark: SparkSession,
        contract: ContractUtils,
        date: String,
        cxiPartnerId: String,
        srcDbName: String,
        srcTable: String,
        destDbName: String
    ): Unit = {

        val itemPriceConfig: Map[String, Any] = contract.prop[Map[String, Any]]("refined.item_price")
        val itemPriceTable = itemPriceConfig("table").asInstanceOf[String]

        val itemsPrices = readItemsPrices(spark, date, srcDbName, srcTable)

        val processedItemsPrices = transformItemsPrices(itemsPrices, cxiPartnerId)

        writeItemsPrices(processedItemsPrices, cxiPartnerId, s"$destDbName.$itemPriceTable")
    }

    def readItemsPrices(spark: SparkSession, date: String, dbName: String, table: String): DataFrame = {
        spark.sql(s"""
               |SELECT
               |get_json_object(record_value, "$$.num") as item_id,
               |loc_ref as location_id,
               |get_json_object(record_value, "$$.effFrDt") as eff_fr_dt,
               |get_json_object(record_value, "$$.prcLvlNum") as prc_lvl_id,
               |get_json_object(record_value, "$$.price") as price,
               |get_json_object(record_value, "$$.rvcNum") as rvc_id
               |FROM $dbName.$table
               |WHERE record_type = "menuItemPrices" AND feed_date = "$date"
               |""".stripMargin)
    }

    def transformItemsPrices(itemsPrices: DataFrame, cxiPartnerId: String): DataFrame = {
        itemsPrices
            .withColumn("cxi_partner_id", lit(cxiPartnerId))
            .withColumn("eff_to_dt", lit(null))
            .withColumn("prc_lvl_name", lit(null))
            .withColumn("amount", lit(null))
            .withColumn("weight", lit(null))
            .withColumn("measurement_unit", lit(null))
            .withColumn("weight_price", lit(null))
            .withColumn("cost", lit(null))
            .dropDuplicates("cxi_partner_id", "item_id", "location_id")
    }

    def writeItemsPrices(df: DataFrame, cxiPartnerId: String, destTable: String): Unit = {
        val srcTable = "newItemsPrices"

        df.createOrReplaceTempView(srcTable)
        df.sqlContext.sql(s"""
               |MERGE INTO $destTable
               |USING $srcTable
               |ON $destTable.cxi_partner_id = "$cxiPartnerId" AND $destTable.item_id = $srcTable.item_id
               |WHEN MATCHED
               |  THEN UPDATE SET *
               |WHEN NOT MATCHED
               |  THEN INSERT *
               |""".stripMargin)
    }
}

object CategoriesProcessor {
    def process(
        spark: SparkSession,
        contract: ContractUtils,
        date: String,
        cxiPartnerId: String,
        srcDbName: String,
        srcTable: String,
        destDbName: String
    ): Unit = {

        val categoryConfig: Map[String, Any] = contract.prop[Map[String, Any]]("refined.category")
        val categoryTable = categoryConfig("table").asInstanceOf[String]

        val categories = readCategories(spark, date, srcDbName, srcTable)

        val processedCategories = transformCategories(categories, cxiPartnerId)

        writeCategories(processedCategories, cxiPartnerId, s"$destDbName.$categoryTable")
    }

    def readCategories(spark: SparkSession, date: String, dbName: String, table: String): DataFrame = {
        spark.sql(s"""
               |SELECT
               |get_json_object(record_value, "$$.majGrpNum") as cat_id,
               |loc_ref as location_id,
               |get_json_object(record_value, "$$.majGrpName") as cat_nm
               |FROM $dbName.$table
               |WHERE record_type = "menuItems" AND feed_date = "$date"
               |""".stripMargin)
    }

    def transformCategories(categories: DataFrame, cxiPartnerId: String): DataFrame = {
        categories
            .withColumn("cxi_partner_id", lit(cxiPartnerId))
            .withColumn("cat_desc", lit(null))
            .dropDuplicates("cxi_partner_id", "cat_id", "location_id")
    }

    def writeCategories(df: DataFrame, cxiPartnerId: String, destTable: String): Unit = {
        val srcTable = "newCategories"

        df.createOrReplaceTempView(srcTable)
        df.sqlContext.sql(s"""
               |MERGE INTO $destTable
               |USING $srcTable
               |ON $destTable.cxi_partner_id = "$cxiPartnerId" AND $destTable.location_id = $srcTable.location_id AND $destTable.cat_id = $srcTable.cat_id
               |WHEN MATCHED
               |  THEN UPDATE SET *
               |WHEN NOT MATCHED
               |  THEN INSERT *
               |""".stripMargin)
    }

}

object OrderTaxesProcessor {
    def process(
        spark: SparkSession,
        contract: ContractUtils,
        date: String,
        cxiPartnerId: String,
        srcDbName: String,
        srcTable: String,
        destDbName: String
    ): Unit = {

        val orderTaxConfig: Map[String, Any] = contract.prop[Map[String, Any]]("refined.order_tax")
        val orderTaxTable = orderTaxConfig("table").asInstanceOf[String]

        val orderTaxes = readOrderTaxes(spark, date, srcDbName, srcTable)

        val processedOrderTaxes = transformOrderTaxes(orderTaxes, cxiPartnerId)

        writeOrderTaxes(processedOrderTaxes, cxiPartnerId, s"$destDbName.$orderTaxTable")
    }

    def readOrderTaxes(spark: SparkSession, date: String, dbName: String, table: String): DataFrame = {
        spark.sql(s"""
               |SELECT
               |get_json_object(record_value, "$$.num") as tax_id,
               |loc_ref as location_id,
               |get_json_object(record_value, "$$.name") as tax_nm,
               |get_json_object(record_value, "$$.taxRate") as tax_rate,
               |get_json_object(record_value, "$$.type") as tax_type
               |FROM $dbName.$table
               |WHERE record_type = "taxes" AND feed_date = "$date"
               |""".stripMargin)
    }

    def transformOrderTaxes(orderTaxes: DataFrame, cxiPartnerId: String): DataFrame = {
        orderTaxes
            .withColumn("cxi_partner_id", lit(cxiPartnerId))
            .dropDuplicates("cxi_partner_id", "location_id", "tax_id")
    }

    def writeOrderTaxes(df: DataFrame, cxiPartnerId: String, destTable: String): Unit = {
        val srcTable = "newOrderTaxes"

        df.createOrReplaceTempView(srcTable)
        df.sqlContext.sql(s"""
               |MERGE INTO $destTable
               |USING $srcTable
               |ON $destTable.cxi_partner_id = "$cxiPartnerId" AND $destTable.location_id = $srcTable.location_id AND $destTable.tax_id = $srcTable.tax_id
               |WHEN MATCHED
               |  THEN UPDATE SET *
               |WHEN NOT MATCHED
               |  THEN INSERT *
               |""".stripMargin)
    }

}

object OrderDiscountsProcessor {
    def process(
        spark: SparkSession,
        contract: ContractUtils,
        date: String,
        cxiPartnerId: String,
        srcDbName: String,
        srcTable: String,
        destDbName: String
    ): Unit = {

        val orderDiscountConfig: Map[String, Any] = contract.prop[Map[String, Any]]("refined.order_discount")
        val orderDiscountTable = orderDiscountConfig("table").asInstanceOf[String]

        val orderDiscounts = readOrderDiscounts(spark, date, srcDbName, srcTable)

        val processedOrderDiscounts = transformOrderDiscounts(orderDiscounts, cxiPartnerId)

        writeOrderDiscounts(processedOrderDiscounts, cxiPartnerId, s"$destDbName.$orderDiscountTable")
    }

    def readOrderDiscounts(spark: SparkSession, date: String, dbName: String, table: String): DataFrame = {
        spark.sql(s"""
               |SELECT
               |get_json_object(record_value, "$$.num") as discount_id,
               |loc_ref as location_id,
               |get_json_object(record_value, "$$.name") as discount_nm,
               |get_json_object(record_value, "$$.posPercent") as percentage
               |FROM $dbName.$table
               |WHERE record_type = "discounts" AND feed_date = "$date"
               |""".stripMargin)
    }

    def transformOrderDiscounts(orderDiscounts: DataFrame, cxiPartnerId: String): DataFrame = {
        orderDiscounts
            .withColumn("cxi_partner_id", lit(cxiPartnerId))
            .dropDuplicates("cxi_partner_id", "location_id", "discount_id")
    }

    def writeOrderDiscounts(df: DataFrame, cxiPartnerId: String, destTable: String): Unit = {
        val srcTable = "newOrderDiscounts"

        df.createOrReplaceTempView(srcTable)
        df.sqlContext.sql(s"""
               |MERGE INTO $destTable
               |USING $srcTable
               |ON $destTable.cxi_partner_id = "$cxiPartnerId" AND $destTable.location_id = $srcTable.location_id
               |  AND $destTable.discount_id = $srcTable.discount_id
               |WHEN MATCHED
               |  THEN UPDATE SET *
               |WHEN NOT MATCHED
               |  THEN INSERT *
               |""".stripMargin)
    }

}

object OrderTenderTypesProcessor {
    def process(
        spark: SparkSession,
        contract: ContractUtils,
        date: String,
        cxiPartnerId: String,
        srcDbName: String,
        srcTable: String,
        destDbName: String
    ): Unit = {

        val orderTenderTypeConfig: Map[String, Any] = contract.prop[Map[String, Any]]("refined.order_tender_type")
        val orderTenderTypeTable = orderTenderTypeConfig("table").asInstanceOf[String]

        val orderTenderTypes = readOrderTenderTypes(spark, date, srcDbName, srcTable)

        val processedOrderTenderTypes = transformOrderTenderTypes(orderTenderTypes, cxiPartnerId)

        writeOrderTenderTypes(processedOrderTenderTypes, cxiPartnerId, s"$destDbName.$orderTenderTypeTable")
    }

    def readOrderTenderTypes(spark: SparkSession, date: String, dbName: String, table: String): DataFrame = {
        spark.sql(s"""
               |SELECT
               |explode(from_json(get_json_object(record_value, "$$.detailLines"), 'array<struct<aggQty:INT, aggTtl:DECIMAL, discount:STRING, dspQty:INT,
               |dspTtl:DECIMAL, lineNum:STRING, menuItem:STRING, tenderMedia:STRING, rsnCodeNum:STRING, svcRndNum:STRING >>')) as details,
               |from_json(details.tenderMedia, 'struct<tmedNum:STRING>').tmedNum as tender_id,
               |loc_ref as location_id,
               |from_json(details.tenderMedia, 'struct<name:STRING>').name as tender_nm
               |FROM $dbName.$table
               |WHERE record_type = "guestChecks" AND feed_date = "$date"
               |""".stripMargin)
    }

    def transformOrderTenderTypes(orderTenderTypes: DataFrame, cxiPartnerId: String): DataFrame = {
        orderTenderTypes
            .filter(col("tender_id").isNotNull)
            .withColumn("cxi_partner_id", lit(cxiPartnerId))
            .withColumn("tender_type", lit(null))
            .dropDuplicates("cxi_partner_id", "location_id", "tender_id")
    }

    def writeOrderTenderTypes(df: DataFrame, cxiPartnerId: String, destTable: String): Unit = {
        val srcTable = "newOrderTenderTypes"

        df.createOrReplaceTempView(srcTable)
        df.sqlContext.sql(s"""
               |MERGE INTO $destTable
               |USING $srcTable
               |ON $destTable.cxi_partner_id = "$cxiPartnerId" AND $destTable.location_id = $srcTable.location_id AND $destTable.tender_id = $srcTable.tender_id
               |WHEN MATCHED
               |  THEN UPDATE SET *
               |WHEN NOT MATCHED
               |  THEN INSERT *
               |""".stripMargin)
    }
}

object OrderServiceChargesProcessor {
    def process(
        spark: SparkSession,
        contract: ContractUtils,
        date: String,
        cxiPartnerId: String,
        srcDbName: String,
        srcTable: String,
        destDbName: String
    ): Unit = {

        val orderServiceChargeConfig: Map[String, Any] = contract.prop[Map[String, Any]]("refined.order_service_charge")
        val orderServiceChargeTable = orderServiceChargeConfig("table").asInstanceOf[String]

        val orderServiceCharges = readOrderServiceCharges(spark, date, srcDbName, srcTable)

        val processedOrderServiceCharges = transformOrderServiceCharges(orderServiceCharges, cxiPartnerId)

        writeOrderServiceCharges(processedOrderServiceCharges, cxiPartnerId, s"$destDbName.$orderServiceChargeTable")
    }

    def readOrderServiceCharges(spark: SparkSession, date: String, dbName: String, table: String): DataFrame = {
        spark.sql(s"""
               |SELECT
               |get_json_object(record_value, "$$.num") as srvc_charge_id,
               |loc_ref as location_id,
               |get_json_object(record_value, "$$.name") as srvc_charge_nm,
               |get_json_object(record_value, "$$.posPercent") as srvc_charge_type
               |FROM $dbName.$table
               |WHERE record_type = "serviceCharges" AND feed_date = "$date"
               |""".stripMargin)
    }

    def transformOrderServiceCharges(orderServiceCharges: DataFrame, cxiPartnerId: String): DataFrame = {
        orderServiceCharges
            .withColumn("cxi_partner_id", lit(cxiPartnerId))
            .dropDuplicates("cxi_partner_id", "location_id", "srvc_charge_id")
    }

    def writeOrderServiceCharges(df: DataFrame, cxiPartnerId: String, destTable: String): Unit = {
        val srcTable = "newOrderServiceCharges"

        df.createOrReplaceTempView(srcTable)
        df.sqlContext.sql(s"""
               |MERGE INTO $destTable
               |USING $srcTable
               |ON $destTable.cxi_partner_id = "$cxiPartnerId" AND $destTable.location_id = $srcTable.location_id
               |  AND $destTable.srvc_charge_id = $srcTable.srvc_charge_id
               |WHEN MATCHED
               |  THEN UPDATE SET *
               |WHEN NOT MATCHED
               |  THEN INSERT *
               |""".stripMargin)
    }
}

// TODO:
//  1. Change schema of `refined_simphony.order_summary` table:
//      cxi_customer_id_array(string) -> cxi_identity_ids ARRAY<STRUCT<identity_type:STRING,cxi_identity_id:String>>
//  2. Include `refined_simphony.order_summary` into `refined_hub.order_summary` view
object OrderSummaryProcessor {
    def process(
        spark: SparkSession,
        contract: ContractUtils,
        date: String,
        cxiPartnerId: String,
        srcDbName: String,
        srcTable: String,
        destDbName: String
    ): Unit = {

        val orderSummaryConfig: Map[String, Any] = contract.prop[Map[String, Any]]("refined.order_summary")
        val orderSummaryTable = orderSummaryConfig("table").asInstanceOf[String]

        val orderSummary = readOrderSummary(spark, date, srcDbName, srcTable)

        val processedOrderSummary = transformOrderSummary(orderSummary, cxiPartnerId)

        writeOrderSummary(processedOrderSummary, cxiPartnerId, s"$destDbName.$orderSummaryTable")
    }

    def readOrderSummary(spark: SparkSession, date: String, dbName: String, table: String): DataFrame = {
        spark.sql(s"""
               |SELECT
               |get_json_object(record_value, "$$.chkNum") as ord_id,
               |get_json_object(record_value, "$$.chkName") as ord_desc,
               |get_json_object(record_value, "$$.chkTtl") as ord_total,
               |get_json_object(record_value, "$$.opnBusDt") as ord_date,
               |get_json_object(record_value, "$$.opnUTC") as ord_timestamp,
               |loc_ref as location_id,
               |get_json_object(record_value, "$$.clsdFlag") as ord_state,
               |explode(from_json(get_json_object(record_value, "$$.detailLines"), 'array<struct<aggQty:INT, aggTtl:DECIMAL, discount:STRING, dspQty:INT,
               |dspTtl:DECIMAL, lineNum:STRING, menuItem:STRING, tenderMedia:STRING, rsnCodeNum:STRING, svcRndNum:STRING >>')) as details,
               |details.aggQty as item_quantity,
               |details.aggTtl as item_total,
               |get_json_object(record_value, "$$.empNum") as emp_id,
               |details.discount as discount_id,
               |details.dspQty as dsp_qty,
               |details.dspTtl as dsp_ttl,
               |details.lineNum as line_id,
               |from_json(details.menuItem, 'struct<activeTaxes:STRING, miNum:STRING, prcLvl:STRING>').activeTaxes as taxes_id,
               |from_json(details.menuItem, 'struct<activeTaxes:STRING, miNum:STRING, prcLvl:STRING>').miNum as item_id,
               |from_json(details.menuItem, 'struct<activeTaxes:STRING, miNum:STRING, prcLvl:STRING>').prcLvl as item_price_id,
               |details.rsnCodeNum as reason_code_id,
               |details.svcRndNum as service_charge_id,
               |from_json(details.tenderMedia, 'struct<tmedNum:STRING>').tmedNum as tender_id,
               |get_json_object(record_value, "$$.guestCheckId") as cxi_customer_id,
               |get_json_object(record_value, "$$.payTtl") as ord_pay_total,
               |get_json_object(record_value, "$$.subTtl") as ord_sub_total
               |FROM $dbName.$table
               |WHERE record_type = "guestChecks" AND feed_date="$date"
               |""".stripMargin)
    }

    def transformOrderSummary(orderSummary: DataFrame, cxiPartnerId: String): DataFrame = {
        orderSummary
            .filter(col("item_id").isNotNull)
            .withColumn("cxi_partner_id", lit(cxiPartnerId))
            .withColumn("ord_type", lit(null))
            .withColumn("ord_originate_channel_id", lit(null))
            .withColumn("ord_target_channel_id", lit(null))
            .withColumn("guest_check_line_item_id", lit(null))
            .withColumn("taxes_amount", lit(null))
            .withColumn("ord_originate_channel_id", lit(null))
            .withColumn("service_charge", lit(null))
            .withColumn(
                "tender_id",
                max("tender_id") over Window
                    .partitionBy("ord_id", "ord_date", "ord_timestamp", "cxi_partner_id", "location_id")
            )
            .select(
                "ord_id",
                "ord_desc",
                "ord_total",
                "ord_date",
                "ord_timestamp",
                "cxi_partner_id",
                "location_id",
                "ord_state",
                "ord_type",
                "ord_originate_channel_id",
                "ord_target_channel_id",
                "item_quantity",
                "item_total",
                "emp_id",
                "discount_id",
                "dsp_qty",
                "dsp_ttl",
                "guest_check_line_item_id",
                "line_id",
                "taxes_id",
                "taxes_amount",
                "item_id",
                "item_price_id",
                "reason_code_id",
                "service_charge_id",
                "tender_id",
                "cxi_customer_id",
                "ord_pay_total",
                "ord_sub_total"
            )
            .dropDuplicates("cxi_partner_id", "location_id", "ord_id", "ord_date")
    }

    def writeOrderSummary(df: DataFrame, cxiPartnerId: String, destTable: String): Unit = {
        val srcTable = "newOrderSummary"

        df.createOrReplaceTempView(srcTable)
        df.sqlContext.sql(s"""
               |MERGE INTO $destTable
               |USING $srcTable
               |ON $destTable.cxi_partner_id = "$cxiPartnerId" AND $destTable.location_id = $srcTable.location_id AND $destTable.ord_id = $srcTable.ord_id
               |  AND $destTable.ord_date = $srcTable.ord_date
               |WHEN MATCHED
               |  THEN UPDATE SET *
               |WHEN NOT MATCHED
               |  THEN INSERT *
               |""".stripMargin)
    }
}
