package com.ringcentral.cassandra4io.cql

import com.datastax.oss.driver.api.core.cql.Row
import shapeless.{ ::, Generic, HList, HNil }

trait ReadsLowestPriority {
  implicit val hNilParser: Reads[HNil] = new Reads[HNil] {
    override def readNullable(row: Row, index: Int): HNil = HNil
    override def read(row: Row, index: Int): HNil         = HNil
    override def nextIndex(index: Int): Int               = index
  }

  implicit def hConsParser[H: Reads, T <: HList: Reads]: Reads[H :: T] = new Reads[H :: T] {

    override def readNullable(row: Row, index: Int): H :: T = {
      val h         = Reads[H].readNullable(row, index)
      val nextIndex = Reads[H].nextIndex(index)
      val t         = Reads[T].readNullable(row, nextIndex)
      h :: t
    }

    override def read(row: Row, index: Int): H :: T = {
      val h         = Reads[H].read(row, index)
      val nextIndex = Reads[H].nextIndex(index)
      val t         = Reads[T].read(row, nextIndex)
      h :: t
    }

    override def nextIndex(index: Int): Int = Reads[T].nextIndex(index)
  }

  implicit def caseClassParser[A, R <: HList](implicit
    gen: Generic[A] { type Repr = R },
    reprParser: Reads[R]
  ): Reads[A] = (row: Row, index: Int) => {
    val rep = reprParser.read(row, index)
    gen.from(rep)
  }
}