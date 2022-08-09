package com.cxi.cdp.data_processing
package refined_zone.hub.geo_identity

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, expr, lit}

import scala.annotation.tailrec

object GeoLocationOverlapPreFiltering {

    private final val LatCol = col("latitude")
    private final val LonCol = col("longitude")

    /** Creates a pushed filter to pre-filter GeoData records in the GeoLocationOverlapJob.
      *
      * Pushed filters are Spark filters that are pushed down to the data source and applied when the data
      * is actually read from the data source. To qualify as a pushed filter, a filter should:
      * - reference the actual fields of the underlying data source (as opposed to transformed / calculated fields)
      *   in our case we should use `latitude` and `longitude`
      * - use simple filtering operations (like numeric comparison)
      * - to be efficient, the number of filters should not be large
      *   this is controlled by the `maxFiltersPerCoordinate` parameter; tests showed optimal values to be around 5
      *
      * The idea is to have a pushed filter that can filter out a significant volume of data at the read time;
      * hopefully a lot of data would not be read at all. It works because Parquet stores records in row groups,
      * each row group maintains statistics / filters for record fields, and given a good pushed filter,
      * entire row groups can be skipped as their statistics / filters do not match a pushed filter.
      *
      * The goal is not to filter out every record that is not in store envelopes, but to filter out as much as possible
      * and maintain good performance. As an example, using this pre-filtering technique, there was more than
      * 3x improvement in job running time.
      *
      * The algorithm is:
      * 1. Create store envelopes from `storeLocationsInMeters` DataFrame.
      *    An envelope is a rectangle with min and max coordinates.
      *    `maxDistanceToStore` parameter (in meters) determines the size of an envelope. It has to be larger than
      *    the largest `maxDistanceToStore` used later in GeoLocationOverlapJob, so something like 100m should be fine.
      *    Note that as we want the resulting filter to be a pushed filter, we create envelopes using lat / lon
      *    in degrees (so we have to transform envelopes in meters (epsg:3857) to envelopes in degrees (epsg:4326)).
      * 2. Using `storeEnvelopes` from the previous step, create an outer bound filter, filtering out records outside
      *    of the enclosing envelope built from `storeEnvelopes` (e.g. filtering out latitudes less than the minimum
      *    latitude of all the envelopes).
      * 3. Using `storeEnvelopes`, and each of coordinates (lat and lon):
      *    - sort envelopes by that coordinate
      *    - calculate gaps between envelopes by that coordinate. E.g. if one envelope has maxLat of 35 degrees, and
      *      the other has minLat of 38 degrees, then the lat gap between them is 3 degrees.
      *    - using `maxFiltersPerCoordinate` widest gaps, create a filter to filter out records that match those gaps
      */
    def storeLocationsPreFilter(
        storeLocationsInMeters: DataFrame,
        maxDistanceToStore: Double,
        maxFiltersPerCoordinate: Int
    )(implicit spark: SparkSession): Column = {
        val storeEnvelopes = extractStoreEnvelopes(storeLocationsInMeters, maxDistanceToStore)
        storeEnvelopePreFilter(storeEnvelopes, maxFiltersPerCoordinate)
    }

    private[geo_identity] def extractStoreEnvelopes(storeLocationsInMeters: DataFrame, maxDistanceToStore: Double)(
        implicit spark: SparkSession
    ): Seq[GeoEnvelope] = {
        import spark.implicits._

        storeLocationsInMeters
            .withColumn(
                "lower_left_meters",
                expr(
                    s"ST_POINT(ST_X(store_location_meters) - $maxDistanceToStore, ST_Y(store_location_meters) - $maxDistanceToStore)"
                )
            )
            .withColumn(
                "upper_right_meters",
                expr(
                    s"ST_POINT(ST_X(store_location_meters) + $maxDistanceToStore, ST_Y(store_location_meters) + $maxDistanceToStore)"
                )
            )
            .withColumn("lower_left", expr(s"ST_TRANSFORM(lower_left_meters, 'epsg:3857', 'epsg:4326')"))
            .withColumn("upper_right", expr(s"ST_TRANSFORM(upper_right_meters, 'epsg:3857', 'epsg:4326')"))
            .select(
                expr("ST_X(lower_left)").as("minLat"),
                expr("ST_Y(lower_left)").as("minLon"),
                expr("ST_X(upper_right)").as("maxLat"),
                expr("ST_Y(upper_right)").as("maxLon")
            )
            .as[GeoEnvelope]
            .distinct
            .collect
            .toSeq
    }

    private[geo_identity] def storeEnvelopePreFilter(
        storeEnvelopes: Seq[GeoEnvelope],
        maxFiltersPerCoordinate: Int
    ): Column = {
        if (storeEnvelopes.isEmpty) {
            lit(true)
        } else {
            val outerBoundsFilter = getOuterBoundsFilter(storeEnvelopes)
            val innerBoundsFilter = getInnerBoundsFilter(storeEnvelopes, maxFiltersPerCoordinate)
            outerBoundsFilter && innerBoundsFilter
        }
    }

    /** Filters out lat/lon that are outside the enclosing envelope of all store envelopes. */
    private def getOuterBoundsFilter(storeEnvelopes: Seq[GeoEnvelope]): Column = {
        // envelope that encloses all store envelopes
        val enclosingEnvelope = getEnclosingEnvelope(storeEnvelopes)

        LatCol >= enclosingEnvelope.minLat && LatCol <= enclosingEnvelope.maxLat && LonCol >= enclosingEnvelope.minLon && LonCol <= enclosingEnvelope.maxLon
    }

    private[geo_identity] def getEnclosingEnvelope(envelopes: Seq[GeoEnvelope]): GeoEnvelope = {
        envelopes.reduce[GeoEnvelope] { case (a, b) =>
            GeoEnvelope(
                minLat = math.min(a.minLat, b.minLat),
                maxLat = math.max(a.maxLat, b.maxLat),
                minLon = math.min(a.minLon, b.minLon),
                maxLon = math.max(a.maxLon, b.maxLon)
            )
        }
    }

    private def getInnerBoundsFilter(storeEnvelopes: Seq[GeoEnvelope], maxFiltersPerCoordinate: Int): Column = {
        val latFilter = getInnerBoundsFilterByCoord(storeEnvelopes, maxFiltersPerCoordinate, LatCol, _.minLat, _.maxLat)
        val lonFilter = getInnerBoundsFilterByCoord(storeEnvelopes, maxFiltersPerCoordinate, LonCol, _.minLon, _.maxLon)
        latFilter && lonFilter
    }

    /** Creates a filter for one coordinate (lat or lon) based on gaps between store envelopes:
      * 1. Calculate gaps between store envelopes based on one coordinate; all input records that are in these gaps
      *    can be safely filtered out.
      * 2. Take `maxFiltersPerCoordinate` widest gaps and filter out records that are in these gaps.
      */
    private def getInnerBoundsFilterByCoord(
        storeEnvelopes: Seq[GeoEnvelope],
        maxFiltersPerCoordinate: Int,
        coordColumn: Column,
        getMinCoord: GeoEnvelope => Double,
        getMaxCoord: GeoEnvelope => Double
    ): Column = {
        val storeIntervals = storeEnvelopes.map(s => Interval(getMinCoord(s), getMaxCoord(s)))
        val mergedSortedStoreIntervals = Interval.mergeOverlappingIntervals(storeIntervals)

        val gapsBetweenStores = mergedSortedStoreIntervals
            .sliding(2)
            .flatMap {
                case Seq(prev, next) => Some(Interval(prev.maxCoord, next.minCoord))
                case _ => None
            }
            .toSeq

        gapsBetweenStores
            .sortBy(-_.size) // use the widest gaps to filter out the most records
            .take(maxFiltersPerCoordinate)
            .map(gap => coordColumn <= gap.minCoord || coordColumn >= gap.maxCoord)
            .reduceOption(_ && _)
            .getOrElse(lit(true))
    }

    private[geo_identity] case class GeoEnvelope(
        minLat: Double,
        maxLat: Double,
        minLon: Double,
        maxLon: Double
    )

    private[geo_identity] case class Interval(minCoord: Double, maxCoord: Double) {
        require(minCoord <= maxCoord)

        def size: Double = maxCoord - minCoord
        def contains(coord: Double): Boolean = {
            minCoord <= coord && coord <= maxCoord
        }
        def isOverlappedBy(other: Interval): Boolean = {
            other.contains(minCoord) || other.contains(maxCoord) || contains(other.minCoord)
        }
    }

    private[geo_identity] object Interval {

        /** Merges overlapping intervals together, if any. Returns merged intervals sorted by coordinates. */
        def mergeOverlappingIntervals(intervals: Seq[Interval]): Seq[Interval] = {
            @tailrec
            def loop(
                alreadyMergedIntervals: Seq[Interval],
                currentInterval: Interval,
                unmergedIntervals: Seq[Interval]
            ): Seq[Interval] = {
                unmergedIntervals match {
                    case head +: tail =>
                        if (currentInterval.isOverlappedBy(head)) {
                            val expandedCurrentInterval = Interval(
                                math.min(currentInterval.minCoord, head.minCoord),
                                math.max(currentInterval.maxCoord, head.maxCoord)
                            )
                            loop(alreadyMergedIntervals, expandedCurrentInterval, tail)
                        } else {
                            loop(alreadyMergedIntervals :+ currentInterval, head, tail)
                        }
                    case _ => alreadyMergedIntervals :+ currentInterval
                }
            }

            val sortedIntervals = intervals.sortBy(_.minCoord)
            sortedIntervals match {
                case head +: tail => loop(Nil, head, tail)
                case _ => Seq.empty
            }
        }
    }

}
