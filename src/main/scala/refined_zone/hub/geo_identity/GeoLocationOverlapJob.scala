package com.cxi.cdp.data_processing
package refined_zone.hub.geo_identity

import com.cxi.cdp.data_processing.support.utils.ContractUtils
import com.cxi.cdp.data_processing.support.SparkSessionFactory

import org.apache.log4j.Logger
import org.apache.sedona.sql.utils._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{broadcast, coalesce, expr, lit, to_date, to_timestamp}

import java.nio.file.Paths

/** This Spark job overlaps our GeoLocation (Veraset) with our Customer Locations (POS-Physical Locations)
  *
  * 1. Read all available locations from refined_hub.location, this should include all partners (view)
  * 2. Create ST Points using Sedona Library to pin-point the locations down to specific meters/horizontal accuracy
  * 3. Overlap the data from our location with the data from GeoLocation to filter out locations/data that is not
  *    in our system. This will reduce the volume of data significantly in preparation for the next phase
  * 4. End Result should be MAID's that are in close proximity to physical locations in our system
  * 5. Full Reprocess mode will account for all of our historic Veraset data against our library of location
  * 6. Non Full Reprocess will pick 1 feed-date and align the data as needed
  */
object GeoLocationOverlapJob {

    private val logger = Logger.getLogger(this.getClass.getName)

    private final val JobConfigPrefix = "jobs.databricks.geo_location_overlap_job.job_config"

    def main(args: Array[String]): Unit = {
        logger.info(s"""Received following args: ${args.mkString(",")}""")

        val cliArgs = CliArgs.parse(args)
        logger.info(s"Parsed args: $cliArgs")

        val spark = SparkSessionFactory.getSparkSession()
        setupSedona(spark)

        run(cliArgs)(spark)
    }

    def run(cliArgs: CliArgs)(implicit spark: SparkSession): Unit = {
        val contract: ContractUtils = new ContractUtils(Paths.get(cliArgs.contractPath))

        val verasetTable = getVerasetTable(contract)
        val locationTable = getLocationTable(contract)
        val geoLocationTable = getGeoLocationTable(contract)

        val horizontalAccuracyMax = contract.prop[Double](s"$JobConfigPrefix.horizontal_accuracy_max")
        val distanceToStore = contract.prop[Double](s"$JobConfigPrefix.distance_to_store")
        val accuracyMultiplier = contract.prop[Double](s"$JobConfigPrefix.accuracy_multiplier")

        val dateExpression: String = getDateFilter(cliArgs.feedDate, cliArgs.fullReprocess)

        val locations = readLocations(locationTable)
        val storeLocationsInMeters = getStoreLocationsInMeters(locations)

        val veraset = readVeraset(verasetTable, dateExpression)

        val geoLocationOverlap = getGeoLocationOverlap(
            horizontalAccuracyMax = horizontalAccuracyMax,
            distanceToStore = distanceToStore,
            accuracyMultiplier = accuracyMultiplier,
            veraset = veraset,
            storeLocationsInMeters = storeLocationsInMeters
        )

        writeGeoLocation(geoLocationOverlap, geoLocationTable, cliArgs.fullReprocess)
    }

    private[geo_identity] def setupSedona(spark: SparkSession): Unit = {
        SedonaSQLRegistrator.registerAll(spark)
    }

    private[geo_identity] def getDateFilter(feedDate: String, fullReprocess: Boolean): String = {
        if (fullReprocess) {
            logger.info("Full reprocess requested")
            "true"
        } else {
            logger.info("Incremental execution requested")
            s"""feed_date = '$feedDate'""".stripMargin
        }
    }

    private def getVerasetTable(contract: ContractUtils): String = {
        contract.prop[String]("schema.raw.veraset_table")
    }

    private def getLocationTable(contract: ContractUtils): String = {
        val db = contract.prop[String]("schema.refined_hub.db_name")
        val table = contract.prop[String]("schema.refined_hub.location_table")
        s"$db.$table"
    }

    private def getGeoLocationTable(contract: ContractUtils): String = {
        val db = contract.prop[String]("schema.refined_hub.db_name")
        val table = contract.prop[String]("schema.refined_hub.geo_location_table")
        s"$db.$table"
    }

    private[geo_identity] def readLocations(table: String)(implicit spark: SparkSession): DataFrame = {
        import spark.implicits._

        def isNonEmpty(column: Column): Column = column.isNotNull && column =!= ""

        spark
            .table(table)
            .where(($"active_flg" === "1") && isNonEmpty($"lat") && isNonEmpty($"long"))
            .select($"cxi_partner_id", $"location_id", $"lat", $"long")
    }

    /** Transforms store locations from degrees (in EPSG:4326) to meters (in EPSG:3857). */
    private[geo_identity] def getStoreLocationsInMeters(
        locations: DataFrame
    )(implicit spark: SparkSession): DataFrame = {
        locations
            .withColumn(
                "store_location_deg",
                expr("ST_POINT(CAST(lat AS DECIMAL(24,20)), CAST(long AS DECIMAL(24,20)))")
            )
            .withColumn("store_location_meters", transformCoordsFromDegreesToMeters("store_location_deg"))
            .select("cxi_partner_id", "location_id", "store_location_meters")
    }

    /** Transforms coordinates from EPSG:4326 (lat / long, in degrees) to EPSG:3857 (coordinates in meters).
      * We need coordinates in meters so that we can calculate a distance between two points in meters later
      * (specifically a distance between a store and a device point).
      *
      * EPSG:4326 uses a coordinate system on the surface of a sphere. Coordinates (lat / long) are measured in degrees.
      * See https://epsg.io/4326 for more details.
      *
      * EPSG:3857 is a projected (flat) coordinate system. Coordinates are measured in meters.
      * See https://epsg.io/3857 for more details.
      */
    private def transformCoordsFromDegreesToMeters(columnName: String): Column = {
        expr(s"ST_TRANSFORM($columnName, 'epsg:4326', 'epsg:3857')")
    }

    private def readVeraset(verasetDataPath: String, dateFilter: String)(implicit spark: SparkSession): DataFrame = {
        import spark.implicits._

        spark.read
            .format("delta")
            .load(verasetDataPath)
            .filter(expr(dateFilter) && ($"advertiser_id_AAID".isNotNull || $"advertiser_id_IDFA".isNotNull))
            .select(
                $"utc_timestamp",
                $"latitude",
                $"longitude",
                $"horizontal_accuracy",
                $"id_type",
                $"advertiser_id_AAID",
                $"advertiser_id_IDFA"
            )
    }

    private[geo_identity] def getGeoLocationOverlap(
        horizontalAccuracyMax: Double,
        distanceToStore: Double,
        accuracyMultiplier: Double,
        storeLocationsInMeters: DataFrame,
        veraset: DataFrame
    )(implicit spark: SparkSession): DataFrame = {
        import spark.implicits._

        veraset
            .where($"horizontal_accuracy" <= lit(horizontalAccuracyMax))
            .withColumn("maid", coalesce($"advertiser_id_AAID", $"advertiser_id_IDFA"))
            .withColumnRenamed("id_type", "maid_type")
            .withColumn("geo_timestamp", to_timestamp($"utc_timestamp"))
            .withColumn("geo_date", to_date($"geo_timestamp"))
            .withColumn("device_point_deg", expr("ST_POINT(latitude, longitude)"))
            .withColumn("device_point_meters", transformCoordsFromDegreesToMeters("device_point_deg"))
            .withColumn(
                "max_distance_to_store",
                ($"horizontal_accuracy" * accuracyMultiplier) + distanceToStore
            )
            .join(
                broadcast(storeLocationsInMeters),
                expr("ST_DISTANCE(device_point_meters, store_location_meters) <= max_distance_to_store"),
                "inner"
            )
            .withColumn("distance_to_store", expr("ST_DISTANCE(device_point_meters, store_location_meters)"))
            .select(
                $"cxi_partner_id",
                $"location_id",
                $"maid",
                $"maid_type",
                $"latitude",
                $"longitude",
                $"horizontal_accuracy",
                $"geo_timestamp",
                $"geo_date",
                $"distance_to_store",
                $"max_distance_to_store"
            )
    }

    // scalastyle:off method.length
    private[geo_identity] def writeGeoLocation(geoLocation: DataFrame, targetTable: String, fullReprocess: Boolean)(
        implicit spark: SparkSession
    ): Unit = {
        if (fullReprocess) {
            logger.info("Full reprocess was requested. Delete old data from the geo_location table.")
            spark.sql(s"DELETE FROM $targetTable")
        }

        val srcTable = "newGeoLocation"
        geoLocation.createOrReplaceTempView(srcTable)
        spark.sql(s"""
               |MERGE INTO $targetTable
               |USING $srcTable
               |ON $targetTable.maid <=> $srcTable.maid
               |   AND $targetTable.maid_type <=> $srcTable.maid_type
               |   AND $targetTable.geo_timestamp <=> $srcTable.geo_timestamp
               |   AND $targetTable.location_id <=> $srcTable.location_id
               |   AND $targetTable.cxi_partner_id <=> $srcTable.cxi_partner_id
               |WHEN MATCHED
               |  THEN UPDATE SET
               |    latitude = $srcTable.latitude,
               |    longitude = $srcTable.longitude,
               |    horizontal_accuracy = $srcTable.horizontal_accuracy,
               |    distance_to_store = $srcTable.distance_to_store,
               |    max_distance_to_store = $srcTable.max_distance_to_store
               |WHEN NOT MATCHED
               |  THEN INSERT (
               |    cxi_partner_id,
               |    location_id,
               |    maid,
               |    maid_type,
               |    latitude,
               |    longitude,
               |    horizontal_accuracy,
               |    geo_timestamp,
               |    geo_date,
               |    distance_to_store,
               |    max_distance_to_store
               |  ) VALUES (
               |    $srcTable.cxi_partner_id,
               |    $srcTable.location_id,
               |    $srcTable.maid,
               |    $srcTable.maid_type,
               |    $srcTable.latitude,
               |    $srcTable.longitude,
               |    $srcTable.horizontal_accuracy,
               |    $srcTable.geo_timestamp,
               |    $srcTable.geo_date,
               |    $srcTable.distance_to_store,
               |    $srcTable.max_distance_to_store
               |  )
            """.stripMargin)
    }

    case class CliArgs(contractPath: String, feedDate: String, fullReprocess: Boolean = false)

    object CliArgs {

        private val initOptions = CliArgs(contractPath = null, feedDate = null, fullReprocess = false)

        private def optionsParser = new scopt.OptionParser[CliArgs]("Geo Location Overlap Job") {

            opt[String]("contract-path")
                .action((contractPath, c) => c.copy(contractPath = contractPath))
                .text("path to a contract for this job")
                .required

            opt[String]("feed-date")
                .action((feedDate, c) => c.copy(feedDate = feedDate))
                .text("feed date to process (format: yyyy-MM-dd)")
                .required

            opt[Boolean]("full-reprocess")
                .action((fullReprocess, c) => c.copy(fullReprocess = fullReprocess))
                .text("if true, remove current location information and reprocess them from the beginning")

        }

        def parse(args: Seq[String]): CliArgs = {
            optionsParser
                .parse(args, initOptions)
                .getOrElse(throw new IllegalArgumentException("Could not parse arguments"))
        }

    }

}
