package com.ringcentral.cassandra4io.codec

import com.ringcentral.cassandra4io.cql.*
import scala.compiletime.constValue
import scala.compiletime.erasedValue
import scala.compiletime.summonInline
import scala.util.NotGiven
import scala.deriving.Mirror
import com.ringcentral.cassandra4io.codec.UdtReads.*
import com.datastax.oss.driver.api.core.data.UdtValue

trait UdtReadsInstances:
  inline given derived[T <: Product: Mirror.ProductOf]: UdtReads[T] =
    inline summonInline[Mirror.ProductOf[T]] match {
      case proMir =>
        instance { udtValue =>
          val fields = recurse[proMir.MirroredElemLabels, proMir.MirroredElemTypes](udtValue)
          proMir.fromProduct(fields)
        }
  }

  private inline def recurse[Names <: Tuple, Types <: Tuple](udtValue: UdtValue): Tuple =
    inline erasedValue[(Names, Types)] match {
      case (_: (name *: names), _: (tpe *: types)) =>
        val fieldName = Configuration.snakeCase(constValue[name].toString)
        val bytes     = udtValue.getBytesUnsafe(fieldName)
        val fieldType = udtValue.getType(fieldName)
        val head      = withRefinedError(summonInline[CellReads[tpe]].read(bytes, udtValue.protocolVersion(), fieldType))(udtValue, fieldName)
        val tail      = recurse[names, types](udtValue)

        head *: tail
      case _ =>
        EmptyTuple
    }

  private def withRefinedError[T](expr: => T)(udtValue: UdtValue, fieldName: String): T =
    try expr
    catch {
      case UnexpectedNullValue.NullValueInColumn => throw UnexpectedNullValue.NullValueInUdt(udtValue, fieldName)
    }

