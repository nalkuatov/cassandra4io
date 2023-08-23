package com.ringcentral.cassandra4io.cql

import scala.util.NotGiven
import shapeless3.deriving.*
import com.datastax.oss.driver.api.core.data.UdtValue
import com.datastax.oss.driver.api.core.`type`.UserDefinedType

trait LowestPriorityToUdtValue:

  given ToUdtValue[EmptyTuple] = ToUdtValue.make((_, constructor) => constructor)

  inline given [H, T <: Tuple](using
    hudt: => ToUdtValue[H],
    tudt: ToUdtValue[T],
    labelling: Labelling[H]
  ): ToUdtValue[H *: T] = ToUdtValue.make[H *: T]: (in: H *: T, constructor: UdtValue) =>
    val head = in.head
    val name = FieldName.Labelled(labelling.label)
    val next = hudt.convert(name, head, constructor)
    tudt.convert(FieldName.Unused, in.tail, next)

  given [A <: Product](using
    gen: K0.ProductGeneric[A],
    evidence: ToUdtValue[gen.MirroredElemTypes]
  ): ToUdtValue.Object[A] = (name, in, constructor) =>
    name match
      case FieldName.Unused => evidence.convert(name, gen.toRepr(in), constructor)
      case FieldName.Labelled(label) => nestedCaseClass(label, gen.toRepr(in), evidence, constructor)

  def nestedCaseClass[A](fieldName: String, in: A, ev: ToUdtValue[A], top: UdtValue): UdtValue = {
    val constructor = top.getType(fieldName).asInstanceOf[UserDefinedType].newValue()
    val serialized  = ev.convert(FieldName.Unused, in, constructor)
    top.setUdtValue(fieldName, serialized)
  }
