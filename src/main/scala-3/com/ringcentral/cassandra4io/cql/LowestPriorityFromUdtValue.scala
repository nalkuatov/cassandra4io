package com.ringcentral.cassandra4io.cql

import com.ringcentral.cassandra4io.cql._
import scala.compiletime.constValue
import shapeless3.deriving.*
import scala.util.NotGiven
import com.datastax.oss.driver.api.core.data.UdtValue

trait LowestPriorityFromUdtValue {
  given fromUdtGen[A <: Product](using
    pInst: K0.ProductInstances[FromUdtValue, A],
    aIsNotOption: NotGiven[A <:< Option[_]]
  ): FromUdtValue.Object[A] = (name, value) =>
    pInst.construct([t] => (from: FromUdtValue[t]) => name match
      case FieldName.Unused => from.convert(name, value)
      case FieldName.Labelled(label) => nestedCaseClass(label, from, value)
    )

  /**
   * Handles the UserDefinedType schema book-keeping before utilizing the Shapeless machinery to inductively derive
   * reading from a UdtValue within a UdtValue
   * @param fieldName is the field name of the nested UdtValue within a given UdtValue
   * @param reader is the mechanism to read a UdtValue into a Scala type A
   * @param top is the top level UdtValue that is used to retrieve the data for the nested UdtValue that resides within it
   * @tparam A is the Scala type A that you want to read from a UdtValue
   * @return
   */
  def nestedCaseClass[A](fieldName: String, reader: FromUdtValue[A], top: UdtValue): A = {
    val nestedUdtValue = top.getUdtValue(fieldName)
    reader.convert(FieldName.Unused, nestedUdtValue)
  }
}
