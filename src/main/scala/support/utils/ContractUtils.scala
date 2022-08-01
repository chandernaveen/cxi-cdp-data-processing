package com.cxi.cdp.data_processing
package support.utils

import support.SparkSessionFactory

import scala.annotation.tailrec
import scala.collection.{immutable, mutable}
import scala.reflect.runtime.universe._
import scala.reflect.ClassTag

class ContractUtils(jsonContent: String) extends Serializable {

    private val properties: immutable.Map[String, Object] = JsonUtils.toMap[Object](jsonContent)

    def this(path: java.nio.file.Path) {
        this(JsonUtils.readFileContentAsString(path.toString)(SparkSessionFactory.getSparkSession()))
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

    private def isInstanceOfMap[T: TypeTag](b: T): Boolean =
        typeOf[mutable.Map[String, Object]] <:< typeOf[T]

    private def get(name: String, map: scala.collection.immutable.Map[String, Object]): Option[Object] = {
        if (map.contains(name)) {
            Some(map(name))
        } else {
            None
        }
    }

    def propIsSet(name: String): Boolean = getProperty(name).isDefined

    def getProperty(name: String): Option[Object] = {
        getProperty(name, properties.toMap)
    }

    @tailrec
    private def getProperty(name: String, map: scala.collection.immutable.Map[String, Object]): Option[Object] = {
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
                                getProperty(
                                    listOfNames.slice(1, listOfNames.size).mkString("."),
                                    value.get.asInstanceOf[scala.collection.immutable.Map[String, Object]]
                                )
                            } else {
                                throw new RuntimeException(s"Unknown Class Name ${value.get.getClass.getName}")
                            }
                    }
                }
                case None => None
            }
        }
    }

    def propToString(name: String): String = {
        propToString(name, properties.toMap)
    }

    private def propToString(name: String, map: scala.collection.immutable.Map[String, Object]): String = {
        if (name == null) {
            throw new IllegalArgumentException("Name is null")
        }
        if (name.isEmpty) {
            JsonUtils.prettyPrinter(properties.toMap)
        } else {
            val listOfNames = name.split("\\.")
            val value = get(listOfNames(0), map)
            value match {
                case Some(s) => {
                    listOfNames.length match {
                        case 1 => s"${listOfNames(0)} : ${JsonUtils.prettyPrinter(value.get)}"
                        case _ => {
                            print(s"${listOfNames(0)}.")
                            if (isInstanceOfMap(value.get)) {
                                val restNamesAsString = propToString(
                                    listOfNames.slice(1, listOfNames.size).mkString("."),
                                    value.get.asInstanceOf[scala.collection.immutable.Map[String, Object]]
                                )
                                s"${listOfNames(0)}.$restNamesAsString"
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

    override def toString: String = properties.toString()
}

object ContractUtils {
    def jobConfigPropName(basePropName: String, relativePropName: String): String = {
        s"$basePropName.$relativePropName"
    }
}
