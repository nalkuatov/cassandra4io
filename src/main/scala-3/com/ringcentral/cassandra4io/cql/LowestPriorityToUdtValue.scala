package com.ringcentral.cassandra4io.cql

import scala.util.NotGiven
import shapeless3.deriving.*
import com.datastax.oss.driver.api.core.data.UdtValue
import com.datastax.oss.driver.api.core.`type`.UserDefinedType

trait LowestPriorityToUdtValue {
  given toUdtGen[A <: Product](using
    pInst: K0.ProductInstances[ToUdtValue, A],
    aIsNotOption: NotGiven[A <:< Option[_]]
  ): ToUdtValue.Object[A] =
    (name, input, value) => name match
      case FieldName.Unused =>
        pInst.foldRight(input)(value)(
          [t] => (to: ToUdtValue[t], in: t, next: UdtValue) => to.convert(name, in, next)
        )
      case FieldName.Labelled(name) =>
        pInst.foldRight(input)(value)(
          [t] => (to: ToUdtValue[t], in: t, next: UdtValue) => nestedCaseClass(name, in, to, next)
        )

  def nestedCaseClass[A](fieldName: String, in: A, ev: ToUdtValue[A], top: UdtValue): UdtValue = {
    val constructor = top.getType(fieldName).asInstanceOf[UserDefinedType].newValue()
    val serialized  = ev.convert(FieldName.Unused, in, constructor)
    top.setUdtValue(fieldName, serialized)
  }
}
