package zio.cassandra.session.cql.codec

import com.datastax.oss.driver.api.core.data.UdtValue
import zio.cassandra.session.cql.codec.UdtWrites.instance

import scala.compiletime.{constValue, erasedValue, summonInline}
import scala.deriving.Mirror

trait UdtWritesInstances1 {

  inline given derived[T <: Product: Mirror.ProductOf](using configuration: Configuration): UdtWrites[T] =
    inline summonInline[Mirror.ProductOf[T]] match {
      case proMir =>
        instance { (t, udtValue) =>
          recurse[proMir.MirroredElemLabels, proMir.MirroredElemTypes](t, udtValue)(0)(configuration)
        }
    }

  private inline def recurse[Names <: Tuple, Types <: Tuple](element: Product, udtValue: UdtValue)(index: Int)(configuration: Configuration): UdtValue =
    inline erasedValue[(Names, Types)] match {
      case (_: (name *: names), _: (tpe *: types)) =>
        val fieldName    = configuration.transformFieldNames(constValue[name].toString)
        val fieldValue   = element.productElement(index).asInstanceOf[tpe]
        val fieldType    = udtValue.getType(fieldName)
        val bytes        = summonInline[CellWrites[tpe]].write(fieldValue, udtValue.protocolVersion(), fieldType)
        val valueWithBytes = udtValue.setBytesUnsafe(fieldName, bytes)
        recurse[names, types](element, valueWithBytes)(index + 1)(configuration)
      case _ =>
        udtValue
    }

}
