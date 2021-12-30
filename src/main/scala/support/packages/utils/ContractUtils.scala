package com.cxi.cdp.data_processing
package support.packages.utils

import scala.collection.mutable.Map
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

class ContractUtils(jsonPath: String) extends Serializable {

  protected var properties: scala.collection.immutable.Map[String, Object] = JsonUtils.toMap[Object](jsonPath)

  /**
    *
    * @param pathToProperyFiles
    */
  def this(pathToProperyFiles: java.nio.file.Path) {
    this(JsonUtils.readJSONSchemaSTR(pathToProperyFiles.toString))
  }

  /**
    *
    * @param name
    * @return
    */
  def getProperty(name: String): Option[Object] = {
    getProperty(name, properties.toMap)
  }

  /**
    *
    * @return
    */
  override def toString(): String = properties.toString()

  /**
    *
    * @param name
    * @return
    */
  def propIsSet(name: String): Boolean = getProperty(name).isDefined

  /**
    *
    * @param name
    * @return
    */
  def propString(name: String): String = {
    val value = getProperty(name, properties.toMap)
    value match {
      case Some(s) => s match {
        case s: String => s
        case _ => throw new RuntimeException(s"Property ${name} is not String type. Type of ${name} is ${value.get.getClass.getName}")
      }
      case None => null
    }
  }

  /**
    *
    * @param name
    * @param alt
    * @return
    */
  def propStringOrElse(name: String, alt: => String): String = Option(propString(name)).getOrElse(alt)

  /**
    *
    * @param name
    * @return
    */
  def propStringOrEmpty(name: String): String = propStringOrElse(name, "")

  /**
    *
    * @param name
    * @return
    */
  def propStringOrNull(name: String): String = propStringOrElse(name, null)

  /**
    *
    * @param name
    * @return
    */
  def propStringOrNone(name: String): Option[String] = Option(propStringOrNull(name))

  /**
    *
    * @param name
    * @return
    */
  def propInt(name: String): Integer = {
    val value = getProperty(name, properties.toMap)
    value match {
      case Some(s) => s match {
        case s: Integer => s
        case _ => throw new RuntimeException(s"Property ${name} is not Integer type. Type of ${name} is ${value.get.getClass.getName}")
      }
      case None => null
    }
  }

  /**
    *
    * @param name
    * @param alt
    * @return
    */
  def propIntOrElse(name: String, alt: => Integer): Integer = Option(propInt(name)).getOrElse(alt)

  /**
    *
    * @param name
    * @return
    */
  def propIntOrEmpty(name: String): Integer = propIntOrElse(name, 0)

  /**
    *
    * @param name
    * @return
    */
  def propIntOrNull(name: String): Integer = propIntOrElse(name, null)

  /**
    *
    * @param name
    * @return
    */
  def propIntOrNone(name: String): Option[Integer] = Option(propIntOrNull(name))

  /**
    *
    * @param name
    * @tparam A
    * @return
    */
  def getListProperty[A: ClassTag](name: String): Option[List[A]] = {
    val value = getProperty(name, properties.toMap)
    value match {
      case Some(s) => s match {
        case s: List[_] => {
          Option(s.asInstanceOf[List[A]])
        }
        case _ => throw new RuntimeException(s"Property ${name} is not List type. Type of ${name} is ${value.get.getClass.getName}")
      }
      case None => None
    }
  }

  def prop[A: ClassTag](name: String): A = {
    val value = getProperty(name, properties.toMap)
    value match {
      case Some(value: A) => value
      case None => throw new IllegalArgumentException(s"Property '${name}' doesn't exists")
      case _ => value.get.asInstanceOf[A]
    }
  }

  def propOrNone[A: ClassTag](name: String): Option[A] = {
    val value = getProperty(name, properties.toMap)
    value match {
      case Some(value: A) => Some(value)
      case None => None
      case _ => Option(value.get.asInstanceOf[A])
    }
  }

  def propOrElse[A: ClassTag](name: String, alt: => A): A = propOrNone[A](name).getOrElse(alt)

  protected def get(name: String, map: scala.collection.immutable.Map[String, Object]): Option[Object] = {
    map.contains(name) match {
      case true => Some(map(name))
      case _ => None
    }
  }

  def isInstanceOfMap[T : TypeTag](b: T): Boolean =
    typeOf[Map[String, Object]] <:< typeOf[T]

  protected def getProperty(name: String, map: scala.collection.immutable.Map[String, Object]): Option[Object] = {
    def gp(listOfNames: Array[String], z: Object) =
      getProperty(listOfNames.slice(1, listOfNames.size).mkString("."), z.asInstanceOf[scala.collection.immutable.Map[String, Object]])

    if (name == null) {
      throw new IllegalArgumentException("Name is null")
    }
      if (name.isEmpty) {
      Some(properties)
    } else {
      val listOfNames = name.split("\\.")
      val value = get(listOfNames(0), map)
      value match {
        case Some(s) => {
          listOfNames.length match {
            case 1 => value
            case _ =>
              if (isInstanceOfMap(value.get)) {
                getProperty(listOfNames.slice(1, listOfNames.size).mkString("."), value.get.asInstanceOf[scala.collection.immutable.Map[String, Object]])
              } else {
                throw new RuntimeException(s"Unknown Class Name ${value.get.getClass.getName}")
              }
            }
        }
        case None => None
      }
    }
  }

  def propToString() : String = {
    propToString("")
  }

  def propToString(name: String) : String = {
    propToString(name, properties.toMap)
  }

  protected def propToString(name: String, map: scala.collection.immutable.Map[String, Object]): String = {
    if (name == null) {
      throw new IllegalArgumentException("Name is null")
    }
      if (name.isEmpty) {
          JsonUtils.prettyPrinter(properties.toMap)
      }
    else {
      val listOfNames = name.split("\\.")
      val value = get(listOfNames(0), map)
      value match {
        case Some(s) => {
          listOfNames.length match {
            case 1 => s"${listOfNames(0)} : ${JsonUtils.prettyPrinter(value.get)}"
            case _ => {
              print(s"${listOfNames(0)}.")
              if (isInstanceOfMap(value.get)) {
                s"${listOfNames(0)}.${propToString(listOfNames.slice(1, listOfNames.size).mkString("."),
                    value.get.asInstanceOf[scala.collection.immutable.Map[String, Object]])}"
              } else {
                throw new RuntimeException(s"Unknown Class Name ${value.get.getClass.getName}")
              }
            }
          }
        }
        case None => ""
      }
    }
  }

}
