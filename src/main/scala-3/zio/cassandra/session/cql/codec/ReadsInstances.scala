package zio.cassandra.session.cql.codec

import com.datastax.oss.driver.api.core.cql.Row
import zio.cassandra.session.cql.codec.Reads.instance

import scala.compiletime.{constValue, erasedValue, summonInline}
import scala.deriving.Mirror

trait ReadsInstances0 extends ReadsInstances1 {

  /** Useful when you want to "cache" [[zio.cassandra.session.cql.codec.Reads]] instance (e.g. to decrease compilation
    * time or make sure it captures the correct [[zio.cassandra.session.cql.codec.Configuration]])
    *
    * Example:
    * {{{
    * final case class Foo(a: Int, b: String)
    *
    * // somewhere else
    * implicit val configuration: Configuration = {
    *   val renameFields: String => String = {
    *     case "a"   => "some_other_name"
    *     case other => Configuration.snakeCaseTransformation(other)
    *     }
    *   Configuration(renameFields)
    * }
    *
    * implicit val reads: Reads[Foo] = Reads.derive
    * }}}
    */
  inline def derive[T <: Product: Mirror.ProductOf](using configuration: Configuration): Reads[T] = derived[T]

}

trait ReadsInstances1 extends ReadsInstances2 {

  given Reads[Row] = instance(identity)

  inline given tupleNReads[T <: Tuple]: Reads[T] =
    instance(recurse[T](_)(0).asInstanceOf[T])

  // return type should be `Types`, but Scala doesn't seem to understand it
  private inline def recurse[Types <: Tuple](row: Row)(index: Int): Tuple =
    inline erasedValue[Types] match {
      case _: (tpe *: types) =>
        val head = readByIndex[tpe](row, index)(using summonInline[CellReads[tpe]])
        val tail = recurse[types](row)(index + 1)

        head *: tail
      case _ =>
        EmptyTuple
    }

}

trait ReadsInstances2 extends ReadsInstances3 {

  inline given derived[T <: Product: Mirror.ProductOf](using configuration: Configuration): Reads[T] =
    inline summonInline[Mirror.ProductOf[T]] match {
      case proMir =>
        instance { row =>
          val fields = recurse[proMir.MirroredElemLabels, proMir.MirroredElemTypes](row)(configuration)
          proMir.fromProduct(fields)
        }
    }

  private inline def recurse[Names <: Tuple, Types <: Tuple](row: Row)(configuration: Configuration): Tuple =
    inline erasedValue[(Names, Types)] match {
      case (_: (name *: names), _: (tpe *: types)) =>
        val fieldName = configuration.transformFieldNames(constValue[name].toString)
        val bytes     = row.getBytesUnsafe(fieldName)
        val fieldType = row.getType(fieldName)
        val head      = withRefinedError(summonInline[CellReads[tpe]].read(bytes, row.protocolVersion(), fieldType))(row, fieldName)
        val tail      = recurse[names, types](row)(configuration)

        head *: tail
      case _ =>
        EmptyTuple
    }

  private def withRefinedError[T](expr: => T)(row: Row, fieldName: String): T =
    try expr
    catch refineError(row, row.getColumnDefinitions.get(fieldName))

}
