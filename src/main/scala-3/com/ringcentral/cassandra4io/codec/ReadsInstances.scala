package com.ringcentral.cassandra4io.codec

import com.datastax.oss.driver.api.core.cql.Row
import com.ringcentral.cassandra4io.codec.Reads
import com.ringcentral.cassandra4io.codec.Reads.*
import scala.compiletime.erasedValue
import scala.compiletime.summonInline
import scala.compiletime.summonAll
import scala.compiletime.constValue
import shapeless3.deriving.*
import scala.util.NotGiven
import scala.deriving.Mirror

trait ReadsInstances extends ReadsInstances2 {

  given Reads[Row] = instance(identity)

  inline given tupleNReads[T <: Tuple]: Reads[T] =
    instance(recurse[T](_)(0).asInstanceOf[T])

  // return type should be `Types`, but Scala doesn't seem to understand it
  private inline def recurse[Types <: Tuple](row: Row)(index: Int): Tuple =
    inline erasedValue[Types] match {
      case _: (tpe *: types) =>
        val head = readByIndex[tpe](row, index)(using summonInline[CellReads[tpe]])
        val tail = recurse[types](row)(index + 1)

        head *: tail
      case _ =>
        EmptyTuple
    }

}

trait ReadsInstances2 extends ReadsInstances1 {

  inline given derived[T <: Product: Mirror.ProductOf]: Reads[T] =
    inline summonInline[Mirror.ProductOf[T]] match {
      case proMir =>
        instance { row =>
          val fields = recurse[proMir.MirroredElemLabels, proMir.MirroredElemTypes](row)
          proMir.fromProduct(fields)
        }
    }

  private inline def recurse[Names <: Tuple, Types <: Tuple](row: Row): Tuple =
    inline erasedValue[(Names, Types)] match {
      case (_: (name *: names), _: (tpe *: types)) =>
        val fieldName = Configuration.snakeCase(constValue[name].toString)
        val bytes     = row.getBytesUnsafe(fieldName)
        val fieldType = row.getType(fieldName)
        val head      = withRefinedError(summonInline[CellReads[tpe]].read(bytes, row.protocolVersion(), fieldType))(row, fieldName)
        val tail      = recurse[names, types](row)

        head *: tail
      case _ =>
        EmptyTuple
    }

  private def withRefinedError[T](expr: => T)(row: Row, fieldName: String): T =
    try expr
    catch refineError(row, row.getColumnDefinitions.get(fieldName))

}