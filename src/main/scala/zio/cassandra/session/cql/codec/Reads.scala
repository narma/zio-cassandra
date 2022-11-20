package zio.cassandra.session.cql.codec

import com.datastax.oss.driver.api.core.cql.{ ColumnDefinition, Row }
import zio.cassandra.session.cql.codec.Reads.instance

/** The main typeclass for decoding Cassandra values, the only one that matters. <br>
  * [[zio.cassandra.session.cql.codec.CellReads]] and [[zio.cassandra.session.cql.codec.UdtReads]] are mostly an
  * implementation details. As long as you can provide and instance of [[zio.cassandra.session.cql.codec.Reads]] for
  * your class (regardless of how you've created it), everything should work just fine.
  */
trait Reads[T] {

  def read(row: Row): T

}

object Reads extends ReadsInstances0 {

  def apply[T](implicit reads: Reads[T]): Reads[T] = reads

  def instance[T](f: Row => T): Reads[T] = (row: Row) => f(row)

  final implicit class ReadsOps[A](private val reads: Reads[A]) extends AnyVal {

    def map[B](f: A => B): Reads[B] = instance(row => f(reads.read(row)))

  }

}

trait ReadsInstances3 {

  protected def refineError(row: Row, columnDefinition: ColumnDefinition): PartialFunction[Throwable, Nothing] = {
    case UnexpectedNullValue.NullValueInColumn                 =>
      throw UnexpectedNullValueInColumn(row, columnDefinition)
    case UnexpectedNullValue.NullValueInUdt(udt, udtFieldName) =>
      throw UnexpectedNullValueInUdt(row, columnDefinition, udt, udtFieldName)
  }

  implicit def readsFromCellReads[T: CellReads]: Reads[T] = instance(readByIndex(_, 0))

  protected def readByIndex[T: CellReads](row: Row, index: Int): T =
    try CellReads[T].read(row.getBytesUnsafe(index), row.protocolVersion(), row.getType(index))
    catch refineError(row, row.getColumnDefinitions.get(index))

}
