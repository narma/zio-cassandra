package zio.cassandra.session.cql.codec

import shapeless.{ ::, HList, HNil, LabelledGeneric, Lazy, Witness }
import shapeless.labelled.FieldType
import zio.cassandra.session.cql.codec.UdtWrites.instance

trait UdtWritesInstances1 {

  implicit val hNilUdtWrites: UdtWrites[HNil] = instance((_, udtValue) => udtValue)

  implicit def hConsUdtWrites[K <: Symbol, H, T <: HList](implicit
    configuration: Configuration,
    hWrites: Lazy[CellWrites[H]],
    tWrites: UdtWrites[T],
    fieldNameW: Witness.Aux[K]
  ): UdtWrites[FieldType[K, H] :: T] =
    instance { (t, udtValue) =>
      val fieldName  = configuration.transformFieldNames(fieldNameW.value.name)
      val hType      = udtValue.getType(fieldName)
      val hBytes     = hWrites.value.write(t.head, udtValue.protocolVersion(), hType)
      val valueWithH = udtValue.setBytesUnsafe(fieldName, hBytes)
      tWrites.write(t.tail, valueWithH)
    }

  implicit def genericUdtWrites[T, Repr](implicit
    configuration: Configuration,
    gen: LabelledGeneric.Aux[T, Repr],
    writes: Lazy[UdtWrites[Repr]]
  ): UdtWrites[T] =
    instance((t, udtValue) => writes.value.write(gen.to(t), udtValue))

}
