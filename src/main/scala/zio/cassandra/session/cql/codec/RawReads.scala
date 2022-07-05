package zio.cassandra.session.cql.codec

import com.datastax.oss.driver.api.core.ProtocolVersion
import com.datastax.oss.driver.api.core.`type`.DataType
import com.datastax.oss.driver.api.core.`type`.codec.TypeCodecs
import com.datastax.oss.driver.api.core.`type`.codec.registry.CodecRegistry
import com.datastax.oss.driver.api.core.`type`.reflect.GenericType
import com.datastax.oss.driver.api.core.data.UdtValue
import com.datastax.oss.driver.internal.core.`type`.{ DefaultListType, DefaultMapType, DefaultSetType }
import zio.cassandra.session.cql.codec.RawReads._
import zio.{ IO, Task, ZIO }

import java.nio.ByteBuffer
import java.time.{ Instant, LocalDate, LocalTime }
import java.util.UUID
import scala.collection.Factory
import scala.jdk.CollectionConverters.{ IterableHasAsScala, MapHasAsScala, SetHasAsScala }
import scala.reflect.{ classTag, ClassTag }

/** Low-level alternative for [[com.datastax.oss.driver.api.core.type.codec.TypeCodec]] that is meant ot be resolved at
  * a compile-time.<br> Its main purpose is to provide a deserializer for a single column value (regardless of if it's a
  * primitive type or an UDT).
  */
trait RawReads[T] {

  def read(bytes: ByteBuffer, protocol: ProtocolVersion, dataType: DataType): IO[RawReads.Error, T]

}

object RawReads extends RawReadsInstances1 {

  sealed trait Error

  final case object UnexpectedNullError extends Error

  final case class InternalError(cause: Throwable) extends Error

  def apply[T](implicit reads: RawReads[T]): RawReads[T] = reads

  def instance[T](f: (ByteBuffer, ProtocolVersion, DataType) => IO[RawReads.Error, T]): RawReads[T] =
    (bytes: ByteBuffer, protocol: ProtocolVersion, dataType: DataType) => f(bytes, protocol, dataType)

  def instance[T](f: (ByteBuffer, ProtocolVersion) => IO[RawReads.Error, T]): RawReads[T] =
    (bytes: ByteBuffer, protocol: ProtocolVersion, _) => f(bytes, protocol)

  implicit class RawReadsOps[A](private val reads: RawReads[A]) extends AnyVal {

    def map[B](f: A => B): RawReads[B] = flatMap(a => ZIO.succeed(f(a)))

    def flatMap[B](f: A => IO[RawReads.Error, B]): RawReads[B] =
      instance((b, p, d) => reads.read(b, p, d).flatMap(f))

  }

  implicit class SafeInstanceOf[T](private val value: T) extends AnyVal {
    def safeInstanceOf[V <: T](implicit vClass: ClassTag[V], tClass: ClassTag[T]): IO[InternalError, V] =
      value match {
        case v: V => ZIO.succeed(v)
        case _    =>
          val expected = tClass.runtimeClass.getSimpleName
          val actual   = vClass.runtimeClass.getSimpleName

          ZIO.fail(InternalError(new IllegalStateException(s"Couldn't convert $expected to $actual")))
      }
  }

}

trait RawReadsInstances1 extends RawReadsInstances2 {

  implicit val stringRawReads: RawReads[String] = fromCodec[String]

  // special case to (hopefully) avoid java boxing (same for other places)
  implicit val booleanRawReads: RawReads[Boolean] = withCheckedNull(TypeCodecs.BOOLEAN.decodePrimitive)

  implicit val shortRawReads: RawReads[Short]   = withCheckedNull(TypeCodecs.SMALLINT.decodePrimitive)
  implicit val intRawReads: RawReads[Int]       = withCheckedNull(TypeCodecs.INT.decodePrimitive)
  implicit val longRawReads: RawReads[Long]     = withCheckedNull(TypeCodecs.BIGINT.decodePrimitive)
  implicit val bigIntRawReads: RawReads[BigInt] = fromCodec[java.math.BigInteger].map(r => r)

  implicit val floatRawReads: RawReads[Float]           = withCheckedNull(TypeCodecs.FLOAT.decodePrimitive)
  implicit val doubleRawReads: RawReads[Double]         = withCheckedNull(TypeCodecs.DOUBLE.decodePrimitive)
  implicit val bigDecimalRawReads: RawReads[BigDecimal] = fromCodec[java.math.BigDecimal].map(r => r)

  implicit val localDateRawReads: RawReads[LocalDate] = fromCodec[LocalDate]
  implicit val localTimeRawReads: RawReads[LocalTime] = fromCodec[LocalTime]
  implicit val instantRawReads: RawReads[Instant]     = fromCodec[Instant]

  implicit val uuidRawReads: RawReads[UUID] = fromCodec[UUID]

  implicit val byteBufferRawReads: RawReads[ByteBuffer] = fromCodec[ByteBuffer]

  private def fromCodec[T: ClassTag]: RawReads[T] = {
    // might throw an exception, but we'd rather immediately die in this case
    // do not inline it, otherwise this search will happen each time when reads is needed
    // ideally we should provide dataType as well for codec search, but we should be fine as is
    val cachedCodec = codecRegistry.codecFor(classTag[T].runtimeClass.asInstanceOf[Class[T]])
    withCheckedNull(cachedCodec.decode)
  }

  private def withCheckedNull[T](f: (ByteBuffer, ProtocolVersion) => T): RawReads[T] =
    instance { (bytes, protocol) =>
      checkNull(bytes) *> Task(f(bytes, protocol)).mapError(InternalError)
    }

}

trait RawReadsInstances2 extends RawReadsInstances3 {

  implicit def optionRawReads[T: RawReads]: RawReads[Option[T]] =
    instance((bytes, protocol, dataType) => ZIO.foreach(Option(bytes))(RawReads[T].read(_, protocol, dataType)))

  implicit def iterableRawReads[T: RawReads, Coll[_] <: Iterable[_]](implicit
    f: Factory[T, Coll[T]]
  ): RawReads[Coll[T]] = {
    val cachedCodec = codecRegistry.codecFor(GenericType.listOf(classOf[ByteBuffer]))
    instance { (bytes, protocol, dataType) =>
      for {
        listType  <- dataType.safeInstanceOf[DefaultListType]
        listElType = listType.getElementType
        r         <- ZIO
                       .foreach(cachedCodec.decode(bytes, protocol).asScala)(RawReads[T].read(_, protocol, listElType))
                       .map(f.fromSpecific)
      } yield r
    }
  }

  implicit def setRawReads[T: RawReads]: RawReads[Set[T]] = {
    val cachedCodec = codecRegistry.codecFor(GenericType.setOf(classOf[ByteBuffer]))
    instance { (bytes, protocol, dataType) =>
      for {
        setType    <- dataType.safeInstanceOf[DefaultSetType]
        setElType   = setType.getElementType
        setOfBytes <- Task(cachedCodec.decode(bytes, protocol).asScala.toSet).mapError(InternalError)
        set        <- ZIO.foreach(setOfBytes)(RawReads[T].read(_, protocol, setElType))
      } yield set
    }
  }

  implicit def mapRawReads[K: RawReads, V: RawReads]: RawReads[Map[K, V]] = {
    val cachedCodec = codecRegistry.codecFor(GenericType.mapOf(classOf[ByteBuffer], classOf[ByteBuffer]))
    instance { (bytes, protocol, dataType) =>
      ZIO.foreach(cachedCodec.decode(bytes, protocol).asScala.toMap) { (key, value) =>
        for {
          mapType  <- dataType.safeInstanceOf[DefaultMapType]
          keyType   = mapType.getKeyType
          valueType = mapType.getValueType
          k        <- RawReads[K].read(key, protocol, keyType)
          v        <- RawReads[V].read(value, protocol, valueType)
        } yield (k, v)
      }
    }
  }

}

trait RawReadsInstances3 {

  protected val codecRegistry: CodecRegistry = CodecRegistry.DEFAULT

  implicit val udtValueRawReads: RawReads[UdtValue] =
    instance { (bytes, protocol, dataType) =>
      val codec = codecRegistry.codecFor(dataType, classOf[UdtValue])
      checkNull(bytes) *> Task(codec.decode(bytes, protocol)).mapError(InternalError)
    }

  implicit def rawReadsFromUdtReads[T: UdtReads]: RawReads[T] =
    RawReads[UdtValue].flatMap(UdtReads[T].read(_).mapError(InternalError))

  def checkNull(bytes: ByteBuffer): IO[UnexpectedNullError.type, Unit] =
    ZIO.fail(UnexpectedNullError).when(bytes == null)

}
