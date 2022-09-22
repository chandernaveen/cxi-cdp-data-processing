package com.cxi.cdp.data_processing
package model.pos_omnivore

import model.pos_omnivore.OmnivoreLandingZoneModel.Embedded

object OmnivoreLandingZoneModel {
    case class Address(
        city: String = null,
        country: String = null,
        state: String = null,
        street1: String = null,
        street2: String = null,
        zip: String = null
    )

    case class Embedded(
        locations: Seq[Location] = null,
        order_types: Seq[OrderType] = null,
        tickets: Seq[TicketsRootInterface] = null
    )

    case class Location(
        address: Address = null,
        display_name: String = null,
        id: String = null,
        name: String = null,
        owner: String = null,
        status: String = null
    )

    case class OrderType(
        available: Boolean,
        id: String = null,
        name: String = null,
        pos_id: String = null
    )

    case class RootInterface(
        _embedded: Embedded = null
    )
    /// *****Ticket*****///

    case class TicketEmbedded(
        items: Seq[Items] = null,
        payments: Seq[Payments] = null
    )

    case class MenuItemEmbedded(
        menu_item: MenuItem = null
    )

    case class CategoryEmbedded(
        menu_categories: Seq[MenuCategories] = null,
        price_levels: Seq[PriceLevels] = null
    )

    case class PaymentEmbedded(
        tender_type: TenderType = null
    )

    case class Items(
        _embedded: MenuItemEmbedded = null,
        id: String = null,
        included_tax: Int,
        name: String = null,
        price: Int,
        quantity: Int,
        seat: Int,
        sent: Boolean,
        sent_at: Int,
        split: Int
    )

    case class MenuCategories(
        id: String = null,
        level: Int,
        name: String = null,
        pos_id: String = null
    )

    case class MenuItem(
        _embedded: CategoryEmbedded = null,
        barcodes: Array[String] = null,
        id: String = null,
        in_stock: Boolean,
        name: String = null,
        open: Boolean,
        open_name: Boolean,
        pos_id: String = null,
        price_per_unit: Int
    )

    case class Payments(
        _embedded: PaymentEmbedded = null,
        amount: Int,
        id: String = null,
        tip: Int,
        _type: String = null
    )

    case class PriceLevels(
        barcodes: Array[String] = null,
        id: String = null,
        name: String = null,
        price_per_unit: Int
    )

    case class TicketsRootInterface(
        _embedded: TicketEmbedded = null,
        auto_send: Boolean,
        closed_at: Int,
        // correlation: Any = null,
        fire_date: String = null,
        fire_time: String = null,
        guest_count: Int,
        id: String = null,
        name: String = null,
        open: Boolean,
        opened_at: Int,
        ticket_number: Int,
        totals: Totals = null,
        _void: Boolean
    )

    case class TenderType(
        allows_tips: Boolean,
        id: String = null,
        name: String = null,
        pos_id: String = null
    )

    case class Totals(
        discounts: Int,
        due: Int,
        exclusive_tax: Int,
        inclusive_tax: Int,
        items: Int,
        other_charges: Int,
        paid: Int,
        service_charges: Int,
        sub_total: Int,
        tax: Int,
        tips: Int,
        total: Int
    )

}

/** A subset of fields from the production Omnivore model to be used in tests.
  * The actual model is much more complex.
  */
case class OmnivoreLandingZoneModel(
    _embedded: Embedded = null,
    feed_date: String = null,
    cxi_id: String = null,
    file_name: String = null
)

case class OmnivoreRawZoneModel(
    record_type: String = null,
    record_value: String = null,
    feed_date: String = null,
    cxi_id: String = null,
    file_name: String = null
)
