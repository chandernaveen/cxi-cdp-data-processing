package com.cxi.cdp.data_processing
package model.parbrink

/** Each model holds a subset of fields from the production Parbrink model to be used in tests.
  * The actual models are much more complex.
  */
object ParbrinkLandingZoneModel {

    case class OrderEntry(
        ItemId: Int,
        GrossSales: Double
    )

    case class OrderPayment(
        Id: Int,
        Amount: Double,
        CardNumber: String
    )

    case class Order(
        Id: Long,
        Entries: Array[OrderEntry],
        Total: Option[Double],
        IsClosed: Boolean,
        Name: String,
        Payments: Array[OrderPayment]
    )

    case class LocationOptions(
        Zip: String,
        Latitude: Double,
        Longitude: Double
    )

    case class Tender(
        Id: Int,
        Active: Boolean,
        Name: String
    )

}

case class ParbrinkLandingZoneModel(
    value: String,
    feed_date: String,
    file_name: String,
    cxi_id: String
)

case class ParbrinkRawZoneModel(
    record_type: String,
    record_value: String,
    location_id: String,
    feed_date: String,
    file_name: String,
    cxi_id: String
)
