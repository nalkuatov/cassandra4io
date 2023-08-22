
package com.ringcentral.cassandra4io.cql

trait BinderLowestPriority {
  implicit val hNilBinder: Binder[HNil]                                   = new Binder[HNil] {
    override def bind(statement: BoundStatement, index: Int, value: HNil): (BoundStatement, Int) = (statement, index)
  }
  implicit def hConsBinder[H: Binder, T <: HList: Binder]: Binder[H :: T] = new Binder[H :: T] {
    override def bind(statement: BoundStatement, index: Int, value: H :: T): (BoundStatement, Int) = {
      val (applied, nextIndex) = Binder[H].bind(statement, index, value.head)
      Binder[T].bind(applied, nextIndex, value.tail)
    }
  }
}