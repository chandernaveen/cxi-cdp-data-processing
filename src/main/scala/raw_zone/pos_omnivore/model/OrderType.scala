package com.cxi.cdp.data_processing
package raw_zone.pos_omnivore.model

case class OrderType(
    _links: SelfLink = null,
    available: Option[Boolean] = Option.empty,
    id: String = null,
    name: String = null,
    pos_id: String = null
)

object OrderType {

    object Name {
        val DineIn = "Dine In"
        val EatIn = "Eat In"
        val CarryOut = "Carry Out"
        val Delivery = "Delivery"
        val OrderAhead = "Order Ahead"
    }

}
