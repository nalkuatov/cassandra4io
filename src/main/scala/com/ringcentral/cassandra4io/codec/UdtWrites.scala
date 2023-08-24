package com.ringcentral.cassandra4io.codec

import com.datastax.oss.driver.api.core.`type`.UserDefinedType
import com.datastax.oss.driver.api.core.cql.BoundStatement
import com.datastax.oss.driver.api.core.data.UdtValue

trait UdtWrites[T] {
  def write(t: T, structure: UdtValue): UdtValue
}

object UdtWrites extends UdtWritesInstances {

  def apply[T](implicit writes: UdtWrites[T]): UdtWrites[T] = writes

  def instance[T](f: (T, UdtValue) => UdtValue): UdtWrites[T] =
    (t: T, udtValue: UdtValue) => f(t, udtValue)

  final implicit class UdtWritesOps[T](private val writes: UdtWrites[T]) extends AnyVal {

    def contramap[V](f: V => T): UdtWrites[V] = instance((t, structure) => writes.write(f(t), structure))

  }

}
