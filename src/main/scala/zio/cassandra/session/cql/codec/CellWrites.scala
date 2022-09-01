package zio.cassandra.session.cql.codec

import com.datastax.oss.driver.api.core.ProtocolVersion
import com.datastax.oss.driver.api.core.`type`.codec.{ ExtraTypeCodecs, TypeCodecs }
import com.datastax.oss.driver.api.core.`type`.codec.registry.CodecRegistry
import com.datastax.oss.driver.api.core.`type`.{ DataType, UserDefinedType }
import com.datastax.oss.driver.api.core.data.UdtValue
import com.datastax.oss.driver.internal.core.`type`.{ DefaultListType, DefaultMapType, DefaultSetType }
import zio.cassandra.session.cql.codec.CellWrites._

import java.nio.ByteBuffer
import java.time.{ Instant, LocalDate, LocalTime }
import java.util.UUID
import scala.jdk.CollectionConverters.{ MapHasAsJava, SeqHasAsJava, SetHasAsJava }

/** Low-level alternative for `com.datastax.oss.driver.api.core.type.codec.TypeCodec` that is meant ot be resolved at a
  * compile-time.<br> Its main purpose is to provide a serializer for a single column value (regardless of if it's a
  * primitive type or an UDT).
  */
trait CellWrites[T] {

  def write(t: T, protocol: ProtocolVersion, dataType: DataType): ByteBuffer

}

object CellWrites extends CellWritesInstances1 {

  def apply[T](implicit writes: CellWrites[T]): CellWrites[T] = writes

  def instance[T](f: (T, ProtocolVersion, DataType) => ByteBuffer): CellWrites[T] =
    (t: T, protocol: ProtocolVersion, dataType: DataType) => f(t, protocol, dataType)

  def instance[T](f: (T, ProtocolVersion) => ByteBuffer): CellWrites[T] =
    (t: T, protocol: ProtocolVersion, _) => f(t, protocol)

  final implicit class CellWritesOps[T](private val writes: CellWrites[T]) extends AnyVal {

    def contramap[V](f: V => T): CellWrites[V] = instance((v, p, d) => writes.write(f(v), p, d))

  }

}

trait CellWritesInstances1 extends CellWritesInstances2 {

  implicit val stringCellWrites: CellWrites[String] = instance_(TypeCodecs.TEXT.encode)

  implicit val booleanCellWrites: CellWrites[Boolean] = instance_(TypeCodecs.BOOLEAN.encodePrimitive)

  implicit val shortCellWrites: CellWrites[Short]   = instance_(TypeCodecs.SMALLINT.encodePrimitive)
  implicit val intCellWrites: CellWrites[Int]       = instance_(TypeCodecs.INT.encodePrimitive)
  implicit val longCellWrites: CellWrites[Long]     = instance_(TypeCodecs.BIGINT.encodePrimitive)
  implicit val bigIntCellWrites: CellWrites[BigInt] = instance_(TypeCodecs.VARINT.encode).contramap(_.bigInteger)

  implicit val floatCellWrites: CellWrites[Float]           = instance_(TypeCodecs.FLOAT.encodePrimitive)
  implicit val doubleCellWrites: CellWrites[Double]         = instance_(TypeCodecs.DOUBLE.encodePrimitive)
  implicit val bigDecimalCellWrites: CellWrites[BigDecimal] =
    instance_(TypeCodecs.DECIMAL.encode).contramap(_.bigDecimal)

  implicit val localDateCellWrites: CellWrites[LocalDate] = instance_(TypeCodecs.DATE.encode)
  implicit val localTimeCellWrites: CellWrites[LocalTime] = instance_(TypeCodecs.TIME.encode)
  implicit val instantCellWrites: CellWrites[Instant]     = instance_(TypeCodecs.TIMESTAMP.encode)

  implicit val uuidCellWrites: CellWrites[UUID] = instance_(TypeCodecs.UUID.encode)

  implicit val byteBufferCellWrites: CellWrites[ByteBuffer] = instance_(TypeCodecs.BLOB.encode)
  implicit val byteArrayCellWrites: CellWrites[Array[Byte]] = instance_(ExtraTypeCodecs.BLOB_TO_ARRAY.encode)

  private def instance_[T](f: (T, ProtocolVersion) => ByteBuffer): CellWrites[T] =
    instance((t, p) => f(t, p))

}

trait CellWritesInstances2 extends CellWritesInstances3 {

  implicit def optionCellWrites[T: CellWrites]: CellWrites[Option[T]] =
    instance { (tOpt, protocol, dataType) =>
      tOpt.map(CellWrites[T].write(_, protocol, dataType)).orNull
    }

  implicit def iterableCellWrites[T: CellWrites, Coll[E] <: Iterable[E]]: CellWrites[Coll[T]] = {
    val cachedCodec = TypeCodecs.listOf(TypeCodecs.BLOB)
    instance { (tColl, protocol, dataType) =>
      val listType    = dataType.asInstanceOf[DefaultListType]
      val listElType  = listType.getElementType
      val collOfBytes = tColl.map(CellWrites[T].write(_, protocol, listElType))
      cachedCodec.encode(collOfBytes.toSeq.asJava, protocol)
    }
  }

  implicit def setCellWrites[T: CellWrites]: CellWrites[Set[T]] = {
    val cachedCodec = TypeCodecs.setOf(TypeCodecs.BLOB)
    instance { (tSet, protocol, dataType) =>
      val setType    = dataType.asInstanceOf[DefaultSetType]
      val setElType  = setType.getElementType
      val setOfBytes = tSet.map(CellWrites[T].write(_, protocol, setElType))
      cachedCodec.encode(setOfBytes.asJava, protocol)
    }
  }

  implicit def mapCellWrites[K: CellWrites, V: CellWrites]: CellWrites[Map[K, V]] = {
    val cachedCodec = TypeCodecs.mapOf(TypeCodecs.BLOB, TypeCodecs.BLOB)
    instance { (tMap, protocol, dataType) =>
      val mapType    = dataType.asInstanceOf[DefaultMapType]
      val keyType    = mapType.getKeyType
      val valueType  = mapType.getValueType
      val mapOfBytes = tMap.map { case (key, value) =>
        val k = CellWrites[K].write(key, protocol, keyType)
        val v = CellWrites[V].write(value, protocol, valueType)
        k -> v
      }
      cachedCodec.encode(mapOfBytes.asJava, protocol)
    }
  }

}

trait CellWritesInstances3 {

  implicit val udtValueCellWrites: CellWrites[UdtValue] =
    instance { (udt, protocol, dataType) =>
      val codec = CodecRegistry.DEFAULT.codecFor(dataType, classOf[UdtValue])
      codec.encode(udt, protocol)
    }

  implicit def cellWritesFromUdtWrites[T: UdtWrites]: CellWrites[T] =
    instance { (t, protocol, dataType) =>
      val udtType   = dataType.asInstanceOf[UserDefinedType]
      val structure = udtType.newValue()
      val udtValue  = UdtWrites[T].write(t, structure)
      CellWrites[UdtValue].write(udtValue, protocol, udtType)
    }

}
