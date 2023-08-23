package com.ringcentral.cassandra4io.cql

import com.datastax.oss.driver.api.core.cql.Row
import shapeless3.deriving.*

trait ReadsLowestPriority {
  inline given Reads[EmptyTuple] with
    def readNullable(row: Row, index: Int): EmptyTuple = EmptyTuple
    override def read(row: Row, index: Int): EmptyTuple  = EmptyTuple
    override def nextIndex(index: Int): Int = index

  inline given [H: Reads, T <: Tuple: Reads]: Reads[H *: T] with
    def readNullable(row: Row, index: Int): H *: T =
      val h         = Reads[H].readNullable(row, index)
      val nextIndex = Reads[H].nextIndex(index)
      val t         = Reads[T].readNullable(row, nextIndex)
      h *: t

    override def read(row: Row, index: Int): H *: T =
      val h         = Reads[H].read(row, index)
      val nextIndex = Reads[H].nextIndex(index)
      val t         = Reads[T].read(row, nextIndex)
      h *: t

    override def nextIndex(index: Int): Int = Reads[T].nextIndex(index)

  given [A <: Product](using
     gen: K0.ProductGeneric[A],
     reads: Reads[gen.MirroredElemTypes]
  ): Reads[A] = (row, index) => gen.fromRepr(reads.read(row, index))

}
