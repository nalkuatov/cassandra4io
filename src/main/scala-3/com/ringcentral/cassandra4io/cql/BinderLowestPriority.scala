package com.ringcentral.cassandra4io.cql

import com.datastax.oss.driver.api.core.cql.BoundStatement

trait BinderLowestPriority {
  given emptyBinder: Binder[EmptyTuple] = new Binder[EmptyTuple] {
    override def bind(statement: BoundStatement, index: Int, value: EmptyTuple): (BoundStatement, Int) = (statement, index)
  }
  implicit def hConsBinder[H: Binder, T <: Tuple: Binder]: Binder[H *: T] = new Binder[H *: T] {
    override def bind(statement: BoundStatement, index: Int, value: H *: T): (BoundStatement, Int) = {
      val (applied, nextIndex) = Binder[H].bind(statement, index, value.head)
      Binder[T].bind(applied, nextIndex, value.tail)
    }
  }
}