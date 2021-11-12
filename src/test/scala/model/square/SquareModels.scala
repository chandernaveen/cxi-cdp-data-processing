package com.cxi.cdp.data_processing
package model.square

import SquareLandingZoneModel._

object SquareLandingZoneModel {

    case class Address(address_line_1: String = null,
                       administrative_district_level_1: String = null,
                       country: String = null,
                       locality: String = null,
                       postal_code: String = null)

    case class BillingAddress(postal_code: String = null)

    case class Card(billing_address: BillingAddress = null,
                    card_brand: String = null,
                    exp_month: Option[Long] = None,
                    exp_year: Option[Long] = None,
                    id: String = null,
                    last_4: String = null)

    case class Preferences(email_unsubscribed: Option[Boolean] = None)

    case class Customer(address: Address = null,
                        birthday: String = null,
                        cards: Seq[Card] = null,
                        created_at: String = null,
                        creation_source: String = null,
                        email_address: String = null,
                        family_name: String = null,
                        given_name: String = null,
                        group_ids: Seq[String] = null,
                        id: String = null,
                        note: String = null,
                        phone_number: String = null,
                        preferences: Preferences = null,
                        reference_id: String = null,
                        segment_ids: Seq[String] = null,
                        updated_at: String = null,
                        version: Option[Long] = None)
}

/** A subset of fields from the production Square model to be used in tests.
  * The actual model is much more complex.
  */
case class SquareLandingZoneModel(customers: Seq[Customer] = null,
                                  feed_date: String = null,
                                  cxi_id: String = null,
                                  file_name: String = null,
                                  cursor: String = null)

case class SquareRawZoneModel(record_type: String = null,
                              record_value: String = null,
                              feed_date: String = null,
                              file_name: String = null,
                              cxi_id: String = null,
                              cursor: String = null)
