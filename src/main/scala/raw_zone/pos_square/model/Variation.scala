package com.cxi.cdp.data_processing
package raw_zone.pos_square.model

case class Variation(absent_at_location_ids: Array[String] = null,
                     catalog_v1_ids: Array[CatalogV1Id] = null,
                     id: String = null,
                     is_deleted: Option[Boolean] = Option.empty,
                     item_variation_data: ItemVariationData = null,
                     present_at_all_locations: Option[Boolean] = Option.empty,
                     present_at_location_ids: Array[String] = null,
                     `type`: String = null,
                     updated_at: String = null,
                     version: Option[Long] = Option.empty)

case class CatalogV1Id(catalog_v1_id: String = null, location_id: String = null)

case class ItemVariationData(available_for_booking: Option[Boolean] = Option.empty,
                             item_id: String = null,
                             location_overrides: Array[LocationOverride] = null,
                             name: String = null,
                             ordinal: Option[Long] = Option.empty,
                             price_money: PriceMoney = null,
                             pricing_type: String = null,
                             sellable: Option[Boolean] = Option.empty,
                             service_duration: Option[Long] = Option.empty,
                             sku: String = null,
                             stockable: Option[Boolean] = Option.empty,
                             track_inventory: Option[Boolean] = Option.empty)

case class LocationOverride(location_id: String = null,
                            price_money: PriceMoney = null,
                            pricing_type: String = null,
                            sold_out: Option[Boolean] = Option.empty,
                            track_inventory: Option[Boolean] = Option.empty)

case class PriceMoney(amount: Option[Long] = Option.empty,
                      currency: String = null)
