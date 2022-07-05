package zio.cassandra.session.cql.codec

import com.datastax.oss.driver.api.core.data.UdtValue
import shapeless._
import shapeless.labelled.{ field, FieldType }
import zio.cassandra.session.cql.codec.UdtReads._
import zio.{ Task, ZIO }

import scala.annotation.nowarn

/** Deserializer created specifically for UDT values.<br> Note that unlike [[zio.cassandra.session.cql.codec.Reads]],
  * this reader can be (is) recursive, so each instance of [[zio.cassandra.session.cql.codec.UdtReads]] can be seen as
  * an instance of [[zio.cassandra.session.cql.codec.RawReads]], while at the same time it might need
  * [[zio.cassandra.session.cql.codec.RawReads]] instances to work.
  */
trait UdtReads[T] {

  def read(udtValue: UdtValue): Task[T]

}

object UdtReads extends UdtReadsInstances1 {

  def apply[T](implicit udtReads: UdtReads[T]): UdtReads[T] = udtReads

  def instance[T](f: UdtValue => Task[T]): UdtReads[T] = (udtValue: UdtValue) => f(udtValue)

}

trait UdtReadsInstances1 {

  implicit val hNilUdtReads: UdtReads[HNil] = instance(_ => ZIO.succeed(HNil))

  implicit def hConsUdtReads[K <: Symbol, H, T <: HList](implicit
    configuration: Configuration,
    hReads: Lazy[RawReads[H]],
    tReads: UdtReads[T],
    fieldNameW: Witness.Aux[K]
  ): UdtReads[FieldType[K, H] :: T] =
    instance { udtValue =>
      for {
        fieldName <- ZIO.succeed(configuration.transformFieldNames(fieldNameW.value.name))
        head      <- hReads.value
                       .read(udtValue.getBytesUnsafe(fieldName), udtValue.protocolVersion(), udtValue.getType(fieldName))
                       .mapError(toThrowable(_, udtValue, fieldName))
        tail      <- tReads.read(udtValue)
      } yield field[K](head) :: tail
    }

  implicit def genericUdtReads[T, Repr](implicit
    // compiler is lying, this configuration is actually used for Reads[Repr] derivation
    @nowarn("msg=never used") configuration: Configuration,
    gen: LabelledGeneric.Aux[T, Repr],
    reads: Lazy[UdtReads[Repr]]
  ): UdtReads[T] =
    instance(udtValue => reads.value.read(udtValue).map(gen.from))

  private def toThrowable(error: RawReads.Error, udtValue: UdtValue, fieldName: String): Throwable =
    error match {
      case RawReads.UnexpectedNullError  => UnexpectedNullValueInUdt(udtValue, fieldName)
      case RawReads.InternalError(cause) => cause
    }

}
