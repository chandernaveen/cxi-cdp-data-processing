package com.cxi.cdp.data_processing
package raw_zone.pos_toast.model

case class Customer(
    guid: Option[String] = Option.empty,
    email: Option[String] = Option.empty,
    phone: Option[String] = Option.empty,
    firstName: Option[String] = Option.empty,
    lastName: Option[String] = Option.empty
)
