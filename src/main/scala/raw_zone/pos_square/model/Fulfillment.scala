package com.cxi.cdp.data_processing
package raw_zone.pos_square.model

case class Fulfillment(pickup_details: PickupDetails = null,
                       state: String = null,
                       `type`: String = null,
                       uid: String = null)

object Fulfillment {

    object State {
        final val Completed = "COMPLETED"
        final val Proposed = "PROPOSED"
    }

    object Type {
        final val Pickup = "PICKUP"
    }

}

case class PickupDetails(accepted_at: String = null,
                         curbside_pickup_details: CurbsidePickupDetails = null,
                         picked_up_at: String = null,
                         note: String = null,
                         pickup_at: String = null,
                         placed_at: String = null,
                         prep_time_duration: String = null,
                         ready_at: String = null,
                         recipient: Recipient = null,
                         schedule_type: String = null)

case class CurbsidePickupDetails(curbside_details: String = null)

case class Recipient(address: Address = null,
                     display_name: String = null,
                     email_address: String = null,
                     phone_number: String = null)

case class Address(address_line_1: String = null,
                   administrative_district_level_1: String = null,
                   country: String = null,
                   locality: String = null,
                   organization: String = null,
                   postal_code: String = null)
