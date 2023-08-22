
package com.ringcentral.cassandra4io.cql

trait LowestPriorityFromUdtValue {
  import shapeless._
  import shapeless.labelled._

  implicit def hListFromUdtValue[K <: Symbol, H, T <: HList](implicit
    witness: Witness.Aux[K],
    hUdtValueReads: => FromUdtValue[H],
    tUdtValueReads: FromUdtValue[T]
  ): FromUdtValue[FieldType[K, H] :: T] = make { (constructor: UdtValue) =>
    val fieldName = FieldName.Labelled(witness.value.name)
    val head      = hUdtValueReads.value.convert(fieldName, constructor)

    val fieldTypeKH: FieldType[K, H] = field[witness.T](head)
    val tail: T                      = tUdtValueReads.convert(FieldName.Unused, constructor)

    fieldTypeKH :: tail
  }

  implicit val hNilFromUdtValue: FromUdtValue[HNil] =
    make((_: UdtValue) => HNil)

  implicit def genericFromUdtValue[A, R](implicit
    gen: LabelledGeneric.Aux[A, R],
    enc: => FromUdtValue[R],
    evidenceANotOption: A <:!< Option[_]
  ): FromUdtValue.Object[A] = { (fieldName: FieldName, udtValue: UdtValue) =>
    fieldName match {
      case FieldName.Unused              => gen.from(enc.convert(fieldName, udtValue))
      case FieldName.Labelled(fieldName) => gen.from(nestedCaseClass(fieldName, enc.value, udtValue))
    }
  }

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
