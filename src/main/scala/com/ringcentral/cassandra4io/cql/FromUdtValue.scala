package com.ringcentral.cassandra4io.cql

import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.oss.driver.api.core.data.UdtValue
import com.ringcentral.cassandra4io.cql.FromUdtValue.{ make, makeWithFieldName }

/**
 * A typeclass that is used to turn a UdtValue into a Scala datatype. Typeclass instances for FromUdtValue
 * are (inductively) derived from CassandraTypeMapper
 *
 * @tparam Scala is the Scala datatype that you intend to read out of a Cassandra UdtValue
 */
trait FromUdtValue[Scala] { self =>
  def convert(fieldName: FieldName, cassandra: UdtValue): Scala

  def map[Scala2](f: Scala => Scala2): FromUdtValue[Scala2] = (fieldName: FieldName, cassandra: UdtValue) =>
    f(self.convert(fieldName, cassandra))
}
object FromUdtValue extends LowerPriorityFromUdtValue with LowestPriorityFromUdtValue {
  trait Object[A] extends FromUdtValue[A]

  def deriveReads[A](implicit ev: FromUdtValue.Object[A]): Reads[A] = (row: Row, index: Int) => {
    val udtValue = row.getUdtValue(index)
    try ev.convert(FieldName.Unused, udtValue)
    catch {
      case UnexpectedNullValueInUdt.NullValueInUdt(udtValue, fieldName) =>
        throw new UnexpectedNullValueInUdt(row, index, udtValue, fieldName)
    }
  }

  // only allowed to summon fully built out FromUdtValue instances which are built by Shapeless machinery
  def apply[A](implicit ev: FromUdtValue.Object[A]): FromUdtValue.Object[A] = ev

  def make[A](mk: UdtValue => A): FromUdtValue[A] =
    (fieldName: FieldName, constructor: UdtValue) =>
      fieldName match {
        case FieldName.Unused =>
          mk(constructor)

        case FieldName.Labelled(value) =>
          throw new RuntimeException(
            s"FromUdtValue failure: Expected an unused fieldName for ${constructor.getType.describe(true)} but got $value"
          )
      }

  def makeWithFieldName[A](mk: (String, UdtValue) => A): FromUdtValue[A] =
    (fieldName: FieldName, constructor: UdtValue) =>
      fieldName match {
        case FieldName.Unused =>
          throw new RuntimeException(
            s"FromUdtValue failure: Expected a labelled fieldName for ${constructor.getType.describe(true)} but got unused"
          )

        case FieldName.Labelled(fieldName) =>
          mk(fieldName, constructor)
      }
}

trait LowerPriorityFromUdtValue {

  /**
   * FromUdtValue relies on the CassandraTypeMapper to convert Scala datatypes into datatypes compatible with the
   * Datastax Java driver (bi-directionally) in order to produce instances of FromUdtValue[A]. CassandraTypeMapper will
   * also inductively derive instances if you have nested data-types (collections within collections or collections
   * within UdtValues within collections or any combinations of these types) inside your UdtValue
   *
   * @param ev is evidence that there exists a CassandraTypeMapper for your Scala datatype A
   * @tparam A is the Scala datatype that must be read out of Cassandra
   * @return
   */
  implicit def deriveFromCassandraTypeMapper[A](implicit
    ev: CassandraTypeMapper[A]
  ): FromUdtValue[A] =
    makeWithFieldName[A] { (fieldName, udtValue) =>
      if (udtValue.isNull(fieldName)) {
        if (ev.allowNullable)
          None.asInstanceOf[A]
        else throw UnexpectedNullValueInUdt.NullValueInUdt(udtValue, fieldName)
      } else
        ev.fromCassandra(udtValue.get(fieldName, ev.classType), udtValue.getType(fieldName))
    }
}

