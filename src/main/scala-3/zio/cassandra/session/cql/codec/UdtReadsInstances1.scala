package zio.cassandra.session.cql.codec

import com.datastax.oss.driver.api.core.data.UdtValue
import zio.cassandra.session.cql.codec.UdtReads.instance

import scala.compiletime.{constValue, erasedValue, summonInline}
import scala.deriving.Mirror

trait UdtReadsInstances1 {

  inline given derived[T <: Product: Mirror.ProductOf](using configuration: Configuration): UdtReads[T] =
    inline summonInline[Mirror.ProductOf[T]] match {
      case proMir =>
        instance { udtValue =>
          val fields = recurse[proMir.MirroredElemLabels, proMir.MirroredElemTypes](udtValue)(configuration)
          proMir.fromProduct(fields)
        }
  }

  private inline def recurse[Names <: Tuple, Types <: Tuple](udtValue: UdtValue)(configuration: Configuration): Tuple =
    inline erasedValue[(Names, Types)] match {
      case (_: (name *: names), _: (tpe *: types)) =>
        val fieldName = configuration.transformFieldNames(constValue[name].toString)
        val bytes     = udtValue.getBytesUnsafe(fieldName)
        val fieldType = udtValue.getType(fieldName)
        val head      = withRefinedError(summonInline[CellReads[tpe]].read(bytes, udtValue.protocolVersion(), fieldType))(udtValue, fieldName)
        val tail      = recurse[names, types](udtValue)(configuration)

        head *: tail
      case _ =>
        EmptyTuple
    }

  private def withRefinedError[T](expr: => T)(udtValue: UdtValue, fieldName: String): T =
    try expr
    catch {
      case UnexpectedNullValue.NullValueInColumn => throw UnexpectedNullValue.NullValueInUdt(udtValue, fieldName)
    }

}
