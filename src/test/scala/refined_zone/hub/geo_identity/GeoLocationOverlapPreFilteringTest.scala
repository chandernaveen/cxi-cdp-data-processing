package com.cxi.cdp.data_processing
package refined_zone.hub.geo_identity

import refined_zone.hub.geo_identity.GeoLocationOverlapJob.setupSedona
import support.BaseSparkBatchJobTest

import org.apache.spark.sql.functions.expr
import org.scalacheck.Gen
import org.scalatest.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class GeoLocationOverlapPreFilteringTest
    extends BaseSparkBatchJobTest
    with Matchers
    with ScalaCheckDrivenPropertyChecks {

    import GeoLocationOverlapPreFiltering._
    import GeoLocationOverlapPreFilteringTest._

    override def beforeAll(): Unit = {
        super.beforeAll()
        setupSedona(spark)
    }

    override implicit val generatorDrivenConfig = PropertyCheckConfiguration(minSuccessful = 20)

    private val latLonGen = for {
        lat <- Gen.chooseNum(-70.0, 70.0)
        lon <- Gen.chooseNum(-170.0, 170.0)
    } yield (lat, lon)

    private val storeEnvelopeGen = for {
        storeLatLon <- latLonGen
        (storeLat, storeLon) = storeLatLon
        maxDistanceToStoreDegrees <- Gen.chooseNum(0.01, 1.0)
    } yield GeoEnvelope(
        minLat = storeLat - maxDistanceToStoreDegrees,
        maxLat = storeLat + maxDistanceToStoreDegrees,
        minLon = storeLon - maxDistanceToStoreDegrees,
        maxLon = storeLon + maxDistanceToStoreDegrees
    )

    test("storeEnvelopePreFilter should keep records that are within store envelopes") {
        import spark.implicits._

        val testCaseGen = for {
            numLatLonRows <- Gen.chooseNum(100, 500)
            numStores <- Gen.chooseNum(0, 10)
            latLonList <- Gen.listOfN(numLatLonRows, latLonGen)
            storeEnvelopes <- Gen.listOfN(numStores, storeEnvelopeGen).map(_.distinct)
            maxFiltersPerCoordinate <- Gen.chooseNum(0, 10)
        } yield (latLonList, storeEnvelopes, maxFiltersPerCoordinate)

        forAll(testCaseGen) { case (latLonList, storeEnvelopes, maxFiltersPerCoordinate) =>
            val preFilter = storeEnvelopePreFilter(storeEnvelopes, maxFiltersPerCoordinate)

            val inputDf = latLonList
                .map { case (lat, lon) =>
                    val matchingStoreEnvelope = storeEnvelopes
                        .find(s => s.minLat <= lat && lat <= s.maxLat && s.minLon <= lon && lon <= s.maxLon)
                    (lat, lon, matchingStoreEnvelope.nonEmpty, matchingStoreEnvelope.toString)
                }
                .toDF("latitude", "longitude", "within_store_envelope", "matching_store_envelope")

            val outputDf = inputDf.withColumn("pre_filter_result", preFilter)

            val incorrectlyFilteredOut = outputDf.filter($"within_store_envelope" && !$"pre_filter_result")
            withClue {
                val renderedDataFrame = incorrectlyFilteredOut.toJSON.collect.mkString("\n")
                s"Some records were incorrectly filtered out:\n$renderedDataFrame"
            } {
                incorrectlyFilteredOut.isEmpty shouldBe true
            }
        }
    }

    test("extractStoreEnvelopes") {
        import spark.implicits._

        val storeLocationsInMetersGen = for {
            x <- Gen.chooseNum(-20000000.0, 20000000.0)
            y <- Gen.chooseNum(-20000000.0, 20000000.0)
        } yield (x, y)

        val testCaseGen = for {
            numStores <- Gen.chooseNum(1, 10)
            storeLocationsInMeters <- Gen.listOfN(numStores, storeLocationsInMetersGen).map(_.distinct)
            maxDistanceToStore <- Gen.chooseNum(1.0, 100.0)
        } yield (storeLocationsInMeters, maxDistanceToStore)

        forAll(testCaseGen) { case (storeLocationsInMeters, maxDistanceToStore) =>
            val storeLocationsInMetersDf = storeLocationsInMeters
                .toDF("x", "y")
                .withColumn("store_location_meters", expr("ST_POINT(x, y)"))

            val storeEnvelopes = extractStoreEnvelopes(storeLocationsInMetersDf, maxDistanceToStore)(spark)

            withClue(s"Number of store envelopes should match the number of store locations") {
                storeEnvelopes.size shouldBe storeLocationsInMeters.size
            }

            // transform store envelopes in lat/lon to meters
            val storeEnvelopesInMeters = storeEnvelopes
                .map(s => (s.minLat, s.minLon, s.maxLat, s.maxLon))
                .toDF("min_lat", "min_lon", "max_lat", "max_lon")
                .withColumn(
                    "lower_left_meters",
                    expr(s"ST_TRANSFORM(ST_POINT(min_lat, min_lon), 'epsg:4326', 'epsg:3857')")
                )
                .withColumn(
                    "upper_right_meters",
                    expr(s"ST_TRANSFORM(ST_POINT(max_lat, max_lon), 'epsg:4326', 'epsg:3857')")
                )
                .select(
                    expr("ST_X(lower_left_meters)").as("minX"),
                    expr("ST_Y(lower_left_meters)").as("minY"),
                    expr("ST_X(upper_right_meters)").as("maxX"),
                    expr("ST_Y(upper_right_meters)").as("maxY")
                )
                .as[StoreEnvelopeInMeters]
                .collect
                .toSeq

            // check that all envelopes have the same size
            storeEnvelopesInMeters.foreach { s =>
                val expectedSize = 2 * maxDistanceToStore +- 0.01 * maxDistanceToStore
                withClue(s"Store envelope $s does not have size of $expectedSize") {
                    (s.maxX - s.minX) shouldBe expectedSize
                    (s.maxY - s.minY) shouldBe expectedSize
                }
            }

            storeEnvelopesInMeters.foreach { storeEnvelope =>
                withClue(s"Store envelope $storeEnvelope does not have any matching stores $storeLocationsInMeters") {
                    val matchFound = storeLocationsInMeters.exists { storeLocation =>
                        storeEnvelope.minX <= storeLocation._1 && storeLocation._1 <= storeEnvelope.maxX &&
                        storeEnvelope.minY <= storeLocation._2 && storeLocation._2 <= storeEnvelope.maxY
                    }
                    matchFound shouldBe true
                }
            }
        }
    }

    test("getEnclosingEnvelope") {
        val testCaseGen = for {
            numStores <- Gen.chooseNum(1, 10)
            storeEnvelopes <- Gen.listOfN(numStores, storeEnvelopeGen)
        } yield storeEnvelopes

        forAll(testCaseGen) { storeEnvelopes =>
            val enclosingEnvelope = getEnclosingEnvelope(storeEnvelopes)
            enclosingEnvelope.minLat shouldBe storeEnvelopes.map(_.minLat).min
            enclosingEnvelope.maxLat shouldBe storeEnvelopes.map(_.maxLat).max
            enclosingEnvelope.minLon shouldBe storeEnvelopes.map(_.minLon).min
            enclosingEnvelope.maxLon shouldBe storeEnvelopes.map(_.maxLon).max
        }
    }

    test("Interval.size") {
        Interval(1.0, 10.0).size shouldBe 9.0
        Interval(1.0, 1.0).size shouldBe 0.0
    }

    test("Interval.contains") {
        val interval = Interval(1.5, 3.5)
        interval.contains(1.5) shouldBe true
        interval.contains(3.5) shouldBe true
        interval.contains(2.0) shouldBe true
        interval.contains(1.4) shouldBe false
        interval.contains(3.6) shouldBe false
        interval.contains(-10.0) shouldBe false
        interval.contains(100) shouldBe false
    }

    test("Interval.isOverlappedBy") {
        val interval = Interval(1.0, 10.0)
        val overlappingIntervals = Seq(
            Interval(2.0, 2.0),
            Interval(2.0, 3.0),
            Interval(8.5, 11.3),
            Interval(0.5, 11.3),
            Interval(0.5, 1.0),
            Interval(1.0, 1.0),
            Interval(10.0, 10.0)
        )
        val nonOverlappingIntervals = Seq(
            Interval(0.5, 0.9),
            Interval(-100.0, -90.0),
            Interval(10.1, 12.5),
            Interval(10.1, 10.1)
        )

        overlappingIntervals.foreach { overlappingInterval =>
            interval.isOverlappedBy(overlappingInterval) shouldBe true
            overlappingInterval.isOverlappedBy(interval) shouldBe true
        }

        nonOverlappingIntervals.foreach { nonOverlappingInterval =>
            interval.isOverlappedBy(nonOverlappingInterval) shouldBe false
            nonOverlappingInterval.isOverlappedBy(interval) shouldBe false
        }
    }

    test("Interval.mergeOverlappingIntervals when there are no overlapping intervals") {
        val intervals = Seq(
            Interval(1.0, 1.0),
            Interval(2.0, 2.5),
            Interval(-5.0, -3.4),
            Interval(12.0, 15.0)
        )

        // the same intervals but sorted
        val expectedMergedIntervals = Seq(
            Interval(-5.0, -3.4),
            Interval(1.0, 1.0),
            Interval(2.0, 2.5),
            Interval(12.0, 15.0)
        )

        Interval.mergeOverlappingIntervals(intervals) shouldBe expectedMergedIntervals
    }

    test("Interval.mergeOverlappingIntervals when there are overlapping intervals") {
        val intervals = Seq(
            // should be merged into one
            Interval(1.0, 1.0),
            Interval(1.0, 1.5),
            Interval(1.3, 1.6),
            // should be merged into one
            Interval(2.0, 2.5),
            // should be merged into one
            Interval(13.0, 14.0),
            Interval(12.0, 15.0),
            // should be merged into one
            Interval(-5.0, -3.4),
            Interval(-7.0, -4.0)
        )

        val expectedMergedIntervals = Seq(
            Interval(-7.0, -3.4),
            Interval(1.0, 1.6),
            Interval(2.0, 2.5),
            Interval(12.0, 15.0)
        )

        Interval.mergeOverlappingIntervals(intervals) shouldBe expectedMergedIntervals
    }

}

object GeoLocationOverlapPreFilteringTest {
    private[GeoLocationOverlapPreFilteringTest] case class StoreEnvelopeInMeters(
        minX: Double,
        minY: Double,
        maxX: Double,
        maxY: Double
    )
}
