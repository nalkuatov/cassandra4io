package com.ringcentral.cassandra4io.codec

import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.oss.driver.api.core.data.UdtValue
import scala.util.control.NoStackTrace
import com.datastax.oss.driver.api.core.cql.ColumnDefinition

sealed trait UnexpectedNullValue extends Throwable

object UnexpectedNullValue {
  // internal intermediate errors that will be enriched with more information later on
  case class NullValueInUdt(udt: UdtValue, fieldName: String) extends NoStackTrace
  case object NullValueInColumn                               extends NoStackTrace
}

case class UnexpectedNullValueInColumn(row: Row, cl: ColumnDefinition) extends RuntimeException() with UnexpectedNullValue {
  override def getMessage: String = {
    val table    = cl.getTable.toString
    val column   = cl.getName.toString
    val keyspace = cl.getKeyspace.toString
    val tpe      = cl.getType.asCql(true, true)

    s"Read NULL value from $keyspace.$table column $column expected $tpe. Row ${row.getFormattedContents}"
  }
}

case class UnexpectedNullValueInUdt(row: Row, cl: ColumnDefinition, udt: UdtValue, fieldName: String)
    extends RuntimeException()
    with UnexpectedNullValue {
  override def getMessage: String = {
    val table    = cl.getTable.toString
    val column   = cl.getName.toString
    val keyspace = cl.getKeyspace.toString
    val tpe      = cl.getType.asCql(true, true)

    val udtTpe = udt.getType(fieldName)

    s"Read NULL value from $keyspace.$table inside UDT column $column with type $tpe. NULL value in $fieldName, expected type $udtTpe. Row ${row.getFormattedContents}"
  }

}

object UnexpectedNullValueInUdt {

  private[codec] case class NullValueInUdt(udtValue: UdtValue, fieldName: String) extends Throwable("", null, true, false)

}
