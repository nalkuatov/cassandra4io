package com.ringcentral.cassandra4io.codec

import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.oss.driver.api.core.cql.ColumnDefinition

trait Reads[T] {

  def read(row: Row): T

}

object Reads extends ReadsInstances {

  def apply[T](implicit reads: Reads[T]): Reads[T] = reads

  def instance[T](f: Row => T): Reads[T] = (row: Row) => f(row)

  final implicit class ReadsOps[A](private val reads: Reads[A]) extends AnyVal {

    def map[B](f: A => B): Reads[B] = instance(row => f(reads.read(row)))

  }

}

trait ReadsInstances1 {

  protected def refineError(row: Row, columnDefinition: ColumnDefinition): PartialFunction[Throwable, Nothing] = {
    case UnexpectedNullValue.NullValueInColumn                 =>
      throw UnexpectedNullValueInColumn(row, columnDefinition)
    case UnexpectedNullValue.NullValueInUdt(udt, udtFieldName) =>
      throw UnexpectedNullValueInUdt(row, columnDefinition, udt, udtFieldName)
  }

  implicit def readsFromCellReads[T: CellReads]: Reads[T] = Reads.instance(readByIndex(_, 0))

  protected def readByIndex[T: CellReads](row: Row, index: Int): T =
    try CellReads[T].read(row.getBytesUnsafe(index), row.protocolVersion(), row.getType(index))
    catch refineError(row, row.getColumnDefinitions.get(index))

}
