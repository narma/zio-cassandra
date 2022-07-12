package zio.cassandra.session.cql.codec

import com.datastax.oss.driver.api.core.cql.{ ColumnDefinition, Row }
import com.datastax.oss.driver.api.core.data.UdtValue

import scala.util.control.NoStackTrace

sealed trait UnexpectedNullValue extends Throwable

object UnexpectedNullValue {
  // internal intermediate errors that will be enriched with more information later on
  case class NullValueInUdt(udt: UdtValue, fieldName: String) extends NoStackTrace
  case object NullValueInColumn                               extends NoStackTrace
}

case class UnexpectedNullValueInColumn(row: Row, cl: ColumnDefinition)
    extends RuntimeException()
    with UnexpectedNullValue {
  override def getMessage: String = {
    val table    = cl.getTable.toString
    val column   = cl.getName.toString
    val keyspace = cl.getKeyspace.toString
    val tpe      = cl.getType.asCql(true, true)

    s"Read NULL value from table [$keyspace.$table] in column [$column], expected type [$tpe]. Row: ${row.getFormattedContents}"
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

    s"Read NULL value from table [$keyspace.$table] in UDT [$column], type [$tpe]. NULL value in field [$fieldName], expected type [$udtTpe]. Row: ${row.getFormattedContents}"
  }

}
