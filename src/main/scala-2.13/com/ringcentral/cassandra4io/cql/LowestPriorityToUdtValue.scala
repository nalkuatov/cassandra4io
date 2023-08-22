package com.ringcentral.cassandra4io.cql

trait LowestPriorityToUdtValue {
  import shapeless._
  import shapeless.labelled._
  implicit def hListToUdtValue[K <: Symbol, H, T <: HList](implicit
    witness: Witness.Aux[K],
    hToUdtValue: => ToUdtValue[H],
    tToUdtValue: ToUdtValue[T]
  ): ToUdtValue[FieldType[K, H] :: T] = make { (in: FieldType[K, H] :: T, constructor: UdtValue) =>
    val headValue       = in.head
    val fieldName       = FieldName.Labelled(witness.value.name)
    val nextConstructor = hToUdtValue.convert(fieldName, headValue, constructor)

    tToUdtValue.convert(FieldName.Unused, in.tail, nextConstructor)
  }

  implicit val hNilToUdtValue: ToUdtValue[HNil] =
    make((_: HNil, constructor: UdtValue) => constructor)

  implicit def genericToUdtValue[A, R](implicit
    gen: LabelledGeneric.Aux[A, R],
    ev: => ToUdtValue[R],
    evidenceANotOption: A <:!< Option[_]
  ): ToUdtValue.Object[A] = { (fieldName: FieldName, in: A, constructor: UdtValue) =>
    fieldName match {
      case FieldName.Unused              => ev.convert(fieldName, gen.to(in), constructor)
      case FieldName.Labelled(fieldName) => nestedCaseClass(fieldName, gen.to(in), ev.value, constructor)
    }
  }

  def nestedCaseClass[A](fieldName: String, in: A, ev: ToUdtValue[A], top: UdtValue): UdtValue = {
    val constructor = top.getType(fieldName).asInstanceOf[UserDefinedType].newValue()
    val serialized  = ev.convert(FieldName.Unused, in, constructor)
    top.setUdtValue(fieldName, serialized)
  }
}
