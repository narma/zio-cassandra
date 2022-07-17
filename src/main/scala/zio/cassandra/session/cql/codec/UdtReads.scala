package zio.cassandra.session.cql.codec

import com.datastax.oss.driver.api.core.data.UdtValue
import shapeless._
import shapeless.labelled.{FieldType, field}
import zio.cassandra.session.cql.codec.UdtReads._

/** Deserializer created specifically for UDT values.<br> Note that unlike [[zio.cassandra.session.cql.codec.Reads]],
  * this reader can be (is) recursive, so each instance of [[zio.cassandra.session.cql.codec.UdtReads]] can be seen as
  * an instance of [[zio.cassandra.session.cql.codec.CellReads]], while at the same time it might need
  * [[zio.cassandra.session.cql.codec.CellReads]] instances to work.
  */
trait UdtReads[T] {

  def read(udtValue: UdtValue): T

}

object UdtReads extends UdtReadsInstances1 {

  def apply[T](implicit udtReads: UdtReads[T]): UdtReads[T] = udtReads

  def instance[T](f: UdtValue => T): UdtReads[T] = (udtValue: UdtValue) => f(udtValue)

}

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
