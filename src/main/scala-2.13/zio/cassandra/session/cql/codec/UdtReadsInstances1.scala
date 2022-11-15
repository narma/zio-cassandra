package zio.cassandra.session.cql.codec

import com.datastax.oss.driver.api.core.data.UdtValue
import shapeless.{ ::, HList, HNil, LabelledGeneric, Lazy, Witness }
import shapeless.labelled.{ field, FieldType }
import zio.cassandra.session.cql.codec.UdtReads.instance

trait UdtReadsInstances1 {

  implicit val hNilUdtReads: UdtReads[HNil] = instance(_ => HNil)

  implicit def hConsUdtReads[K <: Symbol, H, T <: HList](implicit
    configuration: Configuration,
    hReads: Lazy[CellReads[H]],
    tReads: UdtReads[T],
    fieldNameW: Witness.Aux[K]
  ): UdtReads[FieldType[K, H] :: T] =
    instance { udtValue =>
      val fieldName = configuration.transformFieldNames(fieldNameW.value.name)
      val bytes     = udtValue.getBytesUnsafe(fieldName)
      val types     = udtValue.getType(fieldName)

      val head = withRefinedError(hReads.value.read(bytes, udtValue.protocolVersion(), types))(udtValue, fieldName)
      val tail = tReads.read(udtValue)

      field[K](head) :: tail
    }

  implicit def genericUdtReads[T, Repr](implicit
    configuration: Configuration,
    gen: LabelledGeneric.Aux[T, Repr],
    reads: Lazy[UdtReads[Repr]]
  ): UdtReads[T] =
    instance(udtValue => gen.from(reads.value.read(udtValue)))

  private def withRefinedError[T](expr: => T)(udtValue: UdtValue, fieldName: String): T =
    try expr
    catch {
      case UnexpectedNullValue.NullValueInColumn => throw UnexpectedNullValue.NullValueInUdt(udtValue, fieldName)
    }

}
