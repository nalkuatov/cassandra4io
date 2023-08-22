package com.ringcentral.cassandra4io.cql

import com.ringcentral.cassandra4io.cql._
import scala.compiletime.constValue
import shapeless3.deriving.*
import scala.util.NotGiven

trait LowestPriorityFromUdtValue {
  given fromUdtGen[A <: Product](using
    pInst: K0.ProductInstances[FromUdtValue, A],
    aIsNotOption: NotGiven[A <:< Option[_]]
  ): FromUdtValue.Object[A] = (name, value) =>
    pInst.construct([t] => (from: FromUdtValue[t]) => from.convert(name, value))
}
