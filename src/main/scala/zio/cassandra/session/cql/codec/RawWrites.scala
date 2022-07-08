package zio.cassandra.session.cql.codec

import com.datastax.oss.driver.api.core.ProtocolVersion
import com.datastax.oss.driver.api.core.`type`.codec.TypeCodecs
import com.datastax.oss.driver.api.core.`type`.codec.registry.CodecRegistry
import com.datastax.oss.driver.api.core.`type`.{ DataType, UserDefinedType }
import com.datastax.oss.driver.api.core.data.UdtValue
import com.datastax.oss.driver.internal.core.`type`.{ DefaultListType, DefaultMapType, DefaultSetType }
import zio.cassandra.session.cql.codec.RawWrites._

import java.nio.ByteBuffer
import java.time.{ Instant, LocalDate, LocalTime }
import java.util.UUID
import scala.jdk.CollectionConverters.{ MapHasAsJava, SeqHasAsJava, SetHasAsJava }

/** Low-level alternative for [[com.datastax.oss.driver.api.core.type.codec.TypeCodec]] that is meant ot be resolved at
  * a compile-time.<br> Its main purpose is to provide a serializer for a single column value (regardless of if it's a
  * primitive type or an UDT).
  */
trait RawWrites[T] {

  def write(t: T, protocol: ProtocolVersion, dataType: DataType): ByteBuffer

}

object RawWrites extends RawWritesInstances1 {

  def apply[T](implicit writes: RawWrites[T]): RawWrites[T] = writes

  def instance[T](f: (T, ProtocolVersion, DataType) => ByteBuffer): RawWrites[T] =
    (t: T, protocol: ProtocolVersion, dataType: DataType) => f(t, protocol, dataType)

  def instance[T](f: (T, ProtocolVersion) => ByteBuffer): RawWrites[T] =
    (t: T, protocol: ProtocolVersion, _) => f(t, protocol)

  final implicit class RawWritesOps[T](private val rawWrites: RawWrites[T]) extends AnyVal {

    def contramap[V](f: V => T): RawWrites[V] = instance((v, p, d) => rawWrites.write(f(v), p, d))

  }

}

trait RawWritesInstances1 extends RawWritesInstances2 {

  implicit val stringRawWrites: RawWrites[String] = instance_(TypeCodecs.TEXT.encode)

  implicit val booleanRawWrites: RawWrites[Boolean] = instance_(TypeCodecs.BOOLEAN.encodePrimitive)

  implicit val shortRawWrites: RawWrites[Short]   = instance_(TypeCodecs.SMALLINT.encodePrimitive)
  implicit val intRawWrites: RawWrites[Int]       = instance_(TypeCodecs.INT.encodePrimitive)
  implicit val longRawWrites: RawWrites[Long]     = instance_(TypeCodecs.BIGINT.encodePrimitive)
  implicit val bigIntRawWrites: RawWrites[BigInt] = instance_(TypeCodecs.VARINT.encode).contramap(_.bigInteger)

  implicit val floatRawWrites: RawWrites[Float]           = instance_(TypeCodecs.FLOAT.encodePrimitive)
  implicit val doubleRawWrites: RawWrites[Double]         = instance_(TypeCodecs.DOUBLE.encodePrimitive)
  implicit val bigDecimalRawWrites: RawWrites[BigDecimal] = instance_(TypeCodecs.DECIMAL.encode).contramap(_.bigDecimal)

  implicit val localDateRawWrites: RawWrites[LocalDate] = instance_(TypeCodecs.DATE.encode)
  implicit val localTimeRawWrites: RawWrites[LocalTime] = instance_(TypeCodecs.TIME.encode)
  implicit val instantRawWrites: RawWrites[Instant]     = instance_(TypeCodecs.TIMESTAMP.encode)

  implicit val uuidRawWrites: RawWrites[UUID] = instance_(TypeCodecs.UUID.encode)

  implicit val byteBufferRawWrites: RawWrites[ByteBuffer] = instance_(TypeCodecs.BLOB.encode)

  private def instance_[T](f: (T, ProtocolVersion) => ByteBuffer): RawWrites[T] =
    instance((t, p) => f(t, p))

}

trait RawWritesInstances2 extends RawWritesInstances3 {

  implicit def optionRawWrites[T: RawWrites]: RawWrites[Option[T]] =
    instance { (tOpt, protocol, dataType) =>
      tOpt.map(RawWrites[T].write(_, protocol, dataType)).orNull
    }

  implicit def iterableRawWrites[T: RawWrites, Coll[E] <: Iterable[E]]: RawWrites[Coll[T]] = {
    val cachedCodec = TypeCodecs.listOf(TypeCodecs.BLOB)
    instance { (tColl, protocol, dataType) =>
      val listType    = dataType.asInstanceOf[DefaultListType]
      val listElType  = listType.getElementType
      val collOfBytes = tColl.map(RawWrites[T].write(_, protocol, listElType))
      cachedCodec.encode(collOfBytes.toSeq.asJava, protocol)
    }
  }

  implicit def setRawWrites[T: RawWrites]: RawWrites[Set[T]] = {
    val cachedCodec = TypeCodecs.setOf(TypeCodecs.BLOB)
    instance { (tSet, protocol, dataType) =>
      val setType    = dataType.asInstanceOf[DefaultSetType]
      val setElType  = setType.getElementType
      val setOfBytes = tSet.map(RawWrites[T].write(_, protocol, setElType))
      cachedCodec.encode(setOfBytes.asJava, protocol)
    }
  }

  implicit def mapRawReads[K: RawWrites, V: RawWrites]: RawWrites[Map[K, V]] = {
    val cachedCodec = TypeCodecs.mapOf(TypeCodecs.BLOB, TypeCodecs.BLOB)
    instance { (tMap, protocol, dataType) =>
      val mapType    = dataType.asInstanceOf[DefaultMapType]
      val keyType    = mapType.getKeyType
      val valueType  = mapType.getValueType
      val mapOfBytes = tMap.map { case (key, value) =>
        val k = RawWrites[K].write(key, protocol, keyType)
        val v = RawWrites[V].write(value, protocol, valueType)
        k -> v
      }
      cachedCodec.encode(mapOfBytes.asJava, protocol)
    }
  }

}

trait RawWritesInstances3 {

  implicit val udtValueRawWrites: RawWrites[UdtValue] =
    instance { (udt, protocol, dataType) =>
      val codec = CodecRegistry.DEFAULT.codecFor(dataType, classOf[UdtValue])
      codec.encode(udt, protocol)
    }

  implicit def rawWritesFromUdtWrites[T: UdtWrites]: RawWrites[T] =
    instance { (t, protocol, dataType) =>
      val udtType   = dataType.asInstanceOf[UserDefinedType]
      val structure = udtType.newValue()
      val udtValue  = UdtWrites[T].write(t, structure)
      RawWrites[UdtValue].write(udtValue, protocol, udtType)
    }

}
