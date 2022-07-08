package zio.cassandra.session.cql.codec

import com.datastax.oss.driver.api.core.ProtocolVersion
import com.datastax.oss.driver.api.core.`type`.DataType
import com.datastax.oss.driver.api.core.`type`.codec.TypeCodecs
import com.datastax.oss.driver.api.core.`type`.codec.registry.CodecRegistry
import com.datastax.oss.driver.api.core.data.UdtValue
import com.datastax.oss.driver.internal.core.`type`.{ DefaultListType, DefaultMapType, DefaultSetType }
import zio.cassandra.session.cql.codec.RawReads._

import java.nio.ByteBuffer
import java.time.{ Instant, LocalDate, LocalTime }
import java.util.UUID
import scala.collection.Factory
import scala.jdk.CollectionConverters.{ IterableHasAsScala, MapHasAsScala, SetHasAsScala }

/** Low-level alternative for [[com.datastax.oss.driver.api.core.type.codec.TypeCodec]] that is meant ot be resolved at
  * a compile-time.<br> Its main purpose is to provide a deserializer for a single column value (regardless of if it's a
  * primitive type or an UDT).
  */
trait RawReads[T] {

  def read(bytes: ByteBuffer, protocol: ProtocolVersion, dataType: DataType): T

}

object RawReads extends RawReadsInstances1 {

  sealed trait Error

  final case object UnexpectedNullError extends Error

  final case class InternalError(cause: Throwable) extends Error

  def apply[T](implicit reads: RawReads[T]): RawReads[T] = reads

  def instance[T](f: (ByteBuffer, ProtocolVersion, DataType) => T): RawReads[T] =
    (bytes: ByteBuffer, protocol: ProtocolVersion, dataType: DataType) => f(bytes, protocol, dataType)

  def instance[T](f: (ByteBuffer, ProtocolVersion) => T): RawReads[T] =
    (bytes: ByteBuffer, protocol: ProtocolVersion, _) => f(bytes, protocol)

  final implicit class RawReadsOps[A](private val reads: RawReads[A]) extends AnyVal {

    def map[B](f: A => B): RawReads[B] = instance((b, p, d) => f(reads.read(b, p, d)))

  }

}

trait RawReadsInstances1 extends RawReadsInstances2 {

  implicit val stringRawReads: RawReads[String] = withCheckedNull(TypeCodecs.TEXT.decode)

  implicit val booleanRawReads: RawReads[Boolean] = withCheckedNull(TypeCodecs.BOOLEAN.decodePrimitive)

  implicit val shortRawReads: RawReads[Short]   = withCheckedNull(TypeCodecs.SMALLINT.decodePrimitive)
  implicit val intRawReads: RawReads[Int]       = withCheckedNull(TypeCodecs.INT.decodePrimitive)
  implicit val longRawReads: RawReads[Long]     = withCheckedNull(TypeCodecs.BIGINT.decodePrimitive)
  implicit val bigIntRawReads: RawReads[BigInt] = withCheckedNull(TypeCodecs.VARINT.decode).map(r => r)

  implicit val floatRawReads: RawReads[Float]           = withCheckedNull(TypeCodecs.FLOAT.decodePrimitive)
  implicit val doubleRawReads: RawReads[Double]         = withCheckedNull(TypeCodecs.DOUBLE.decodePrimitive)
  implicit val bigDecimalRawReads: RawReads[BigDecimal] = withCheckedNull(TypeCodecs.DECIMAL.decode).map(r => r)

  implicit val localDateRawReads: RawReads[LocalDate] = withCheckedNull(TypeCodecs.DATE.decode)
  implicit val localTimeRawReads: RawReads[LocalTime] = withCheckedNull(TypeCodecs.TIME.decode)
  implicit val instantRawReads: RawReads[Instant]     = withCheckedNull(TypeCodecs.TIMESTAMP.decode)

  implicit val uuidRawReads: RawReads[UUID] = withCheckedNull(TypeCodecs.UUID.decode)

  implicit val byteBufferRawReads: RawReads[ByteBuffer] = withCheckedNull(TypeCodecs.BLOB.decode)

  private def withCheckedNull[T](f: (ByteBuffer, ProtocolVersion) => T): RawReads[T] =
    instance { (bytes, protocol) =>
      requireNonNull(bytes)
      f(bytes, protocol)
    }

}

trait RawReadsInstances2 extends RawReadsInstances3 {

  implicit def optionRawReads[T: RawReads]: RawReads[Option[T]] =
    instance((bytes, protocol, dataType) => Option(bytes).map(RawReads[T].read(_, protocol, dataType)))

  implicit def iterableRawReads[T: RawReads, Coll[_] <: Iterable[_]](implicit
    f: Factory[T, Coll[T]]
  ): RawReads[Coll[T]] = {
    val cachedCodec = TypeCodecs.listOf(TypeCodecs.BLOB)
    instance { (bytes, protocol, dataType) =>
      val listType   = dataType.asInstanceOf[DefaultListType]
      val listElType = listType.getElementType
      val elements = cachedCodec.decode(bytes, protocol).asScala.map(RawReads[T].read(_, protocol, listElType))
      f.fromSpecific(elements)
    }
  }

  implicit def setRawReads[T: RawReads]: RawReads[Set[T]] = {
    val cachedCodec = TypeCodecs.setOf(TypeCodecs.BLOB)
    instance { (bytes, protocol, dataType) =>
      val setType    = dataType.asInstanceOf[DefaultSetType]
      val setElType  = setType.getElementType
      val setOfBytes = cachedCodec.decode(bytes, protocol).asScala.toSet
      setOfBytes.map(RawReads[T].read(_, protocol, setElType))
    }
  }

  implicit def mapRawReads[K: RawReads, V: RawReads]: RawReads[Map[K, V]] = {
    val cachedCodec = TypeCodecs.mapOf(TypeCodecs.BLOB, TypeCodecs.BLOB)
    instance { (bytes, protocol, dataType) =>
      cachedCodec.decode(bytes, protocol).asScala.toMap.map { case (key, value) =>
        val mapType   = dataType.asInstanceOf[DefaultMapType]
        val keyType   = mapType.getKeyType
        val valueType = mapType.getValueType
        val k         = RawReads[K].read(key, protocol, keyType)
        val v         = RawReads[V].read(value, protocol, valueType)
        (k, v)
      }
    }
  }

}

trait RawReadsInstances3 {

  protected val codecRegistry: CodecRegistry = CodecRegistry.DEFAULT

  implicit val udtValueRawReads: RawReads[UdtValue] =
    instance { (bytes, protocol, dataType) =>
      requireNonNull(bytes)
      CodecRegistry.DEFAULT.codecFor(dataType, classOf[UdtValue]).decode(bytes, protocol)
    }

  implicit def rawReadsFromUdtReads[T: UdtReads]: RawReads[T] =
    RawReads[UdtValue].map(UdtReads[T].read(_))

  def requireNonNull(bytes: ByteBuffer): Unit = if (bytes == null) throw UnexpectedNullValue.NullValueInColumn

}
