package com.ringcentral.cassandra4io.codec

import shapeless3.deriving.*
import com.datastax.oss.driver.api.core.data.UdtValue
import com.datastax.oss.driver.api.core.`type`.UserDefinedType
import com.ringcentral.cassandra4io.codec.UdtWrites.*
import scala.compiletime.summonInline
import scala.compiletime.erasedValue
import scala.compiletime.constValue
import scala.deriving.Mirror

trait UdtWritesInstances:
  inline given derived[T <: Product: Mirror.ProductOf]: UdtWrites[T] =
    inline summonInline[Mirror.ProductOf[T]] match {
      case proMir =>
        instance { (t, udtValue) =>
          recurse[proMir.MirroredElemLabels, proMir.MirroredElemTypes](t, udtValue)(0)
        }
    }

  private inline def recurse[Names <: Tuple, Types <: Tuple](element: Product, udtValue: UdtValue)(index: Int): UdtValue =
    inline erasedValue[(Names, Types)] match {
      case (_: (name *: names), _: (tpe *: types)) =>
        val fieldName    = constValue[name].toString
        val fieldValue   = element.productElement(index).asInstanceOf[tpe]
        val fieldType    = udtValue.getType(fieldName)
        val bytes        = summonInline[CellWrites[tpe]].write(fieldValue, udtValue.protocolVersion(), fieldType)
        val valueWithBytes = udtValue.setBytesUnsafe(fieldName, bytes)
        recurse[names, types](element, valueWithBytes)(index + 1)
      case _ =>
        udtValue
    }
