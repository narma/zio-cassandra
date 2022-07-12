package zio.cassandra.session.cql.codec

import com.datastax.oss.driver.api.core.ProtocolVersion
import com.datastax.oss.driver.api.core.`type`.DataType
import com.datastax.oss.driver.api.core.`type`.codec.TypeCodecs
import com.datastax.oss.driver.api.core.`type`.codec.registry.CodecRegistry
import com.datastax.oss.driver.api.core.data.UdtValue
import com.datastax.oss.driver.internal.core.`type`.{ DefaultListType, DefaultMapType, DefaultSetType }
import zio.cassandra.session.cql.codec.CellReads._

import java.nio.ByteBuffer
import java.time.{ Instant, LocalDate, LocalTime }
import java.util.UUID
import scala.collection.Factory
import scala.jdk.CollectionConverters.{ IterableHasAsScala, MapHasAsScala, SetHasAsScala }

/** Low-level alternative for [[com.datastax.oss.driver.api.core.type.codec.TypeCodec]] that is meant ot be resolved at
  * a compile-time.<br> Its main purpose is to provide a deserializer for a single column value (regardless of if it's a
  * primitive type or an UDT).
  */
trait CellReads[T] {

  def read(bytes: ByteBuffer, protocol: ProtocolVersion, dataType: DataType): T

}

object CellReads extends CellReadsInstances1 {

  def apply[T](implicit reads: CellReads[T]): CellReads[T] = reads

  def instance[T](f: (ByteBuffer, ProtocolVersion, DataType) => T): CellReads[T] =
    (bytes: ByteBuffer, protocol: ProtocolVersion, dataType: DataType) => f(bytes, protocol, dataType)

  def instance[T](f: (ByteBuffer, ProtocolVersion) => T): CellReads[T] =
    (bytes: ByteBuffer, protocol: ProtocolVersion, _) => f(bytes, protocol)

  final implicit class CellReadsOps[A](private val reads: CellReads[A]) extends AnyVal {

    def map[B](f: A => B): CellReads[B] = instance((b, p, d) => f(reads.read(b, p, d)))

  }

}

trait CellReadsInstances1 extends CellReadsInstances2 {

  implicit val stringCellReads: CellReads[String] = withCheckedNull(TypeCodecs.TEXT.decode)

  implicit val booleanCellReads: CellReads[Boolean] = withCheckedNull(TypeCodecs.BOOLEAN.decodePrimitive)

  implicit val shortCellReads: CellReads[Short]   = withCheckedNull(TypeCodecs.SMALLINT.decodePrimitive)
  implicit val intCellReads: CellReads[Int]       = withCheckedNull(TypeCodecs.INT.decodePrimitive)
  implicit val longCellReads: CellReads[Long]     = withCheckedNull(TypeCodecs.BIGINT.decodePrimitive)
  implicit val bigIntCellReads: CellReads[BigInt] = withCheckedNull(TypeCodecs.VARINT.decode).map(r => r)

  implicit val floatCellReads: CellReads[Float]           = withCheckedNull(TypeCodecs.FLOAT.decodePrimitive)
  implicit val doubleCellReads: CellReads[Double]         = withCheckedNull(TypeCodecs.DOUBLE.decodePrimitive)
  implicit val bigDecimalCellReads: CellReads[BigDecimal] = withCheckedNull(TypeCodecs.DECIMAL.decode).map(r => r)

  implicit val localDateCellReads: CellReads[LocalDate] = withCheckedNull(TypeCodecs.DATE.decode)
  implicit val localTimeCellReads: CellReads[LocalTime] = withCheckedNull(TypeCodecs.TIME.decode)
  implicit val instantCellReads: CellReads[Instant]     = withCheckedNull(TypeCodecs.TIMESTAMP.decode)

  implicit val uuidCellReads: CellReads[UUID] = withCheckedNull(TypeCodecs.UUID.decode)

  implicit val byteBufferCellReads: CellReads[ByteBuffer] = withCheckedNull(TypeCodecs.BLOB.decode)

  private def withCheckedNull[T](f: (ByteBuffer, ProtocolVersion) => T): CellReads[T] =
    instance { (bytes, protocol) =>
      requireNonNull(bytes)
      f(bytes, protocol)
    }

}

trait CellReadsInstances2 extends CellReadsInstances3 {

  implicit def optionCellReads[T: CellReads]: CellReads[Option[T]] =
    instance((bytes, protocol, dataType) => Option(bytes).map(CellReads[T].read(_, protocol, dataType)))

  implicit def iterableCellReads[T: CellReads, Coll[_] <: Iterable[_]](implicit
    f: Factory[T, Coll[T]]
  ): CellReads[Coll[T]] = {
    val cachedCodec = TypeCodecs.listOf(TypeCodecs.BLOB)
    instance { (bytes, protocol, dataType) =>
      val listType   = dataType.asInstanceOf[DefaultListType]
      val listElType = listType.getElementType
      val elements = cachedCodec.decode(bytes, protocol).asScala.map(CellReads[T].read(_, protocol, listElType))
      f.fromSpecific(elements)
    }
  }

  implicit def setCellReads[T: CellReads]: CellReads[Set[T]] = {
    val cachedCodec = TypeCodecs.setOf(TypeCodecs.BLOB)
    instance { (bytes, protocol, dataType) =>
      val setType    = dataType.asInstanceOf[DefaultSetType]
      val setElType  = setType.getElementType
      val setOfBytes = cachedCodec.decode(bytes, protocol).asScala.toSet
      setOfBytes.map(CellReads[T].read(_, protocol, setElType))
    }
  }

  implicit def mapCellReads[K: CellReads, V: CellReads]: CellReads[Map[K, V]] = {
    val cachedCodec = TypeCodecs.mapOf(TypeCodecs.BLOB, TypeCodecs.BLOB)
    instance { (bytes, protocol, dataType) =>
      cachedCodec.decode(bytes, protocol).asScala.toMap.map { case (key, value) =>
        val mapType   = dataType.asInstanceOf[DefaultMapType]
        val keyType   = mapType.getKeyType
        val valueType = mapType.getValueType
        val k         = CellReads[K].read(key, protocol, keyType)
        val v         = CellReads[V].read(value, protocol, valueType)
        (k, v)
      }
    }
  }

}

trait CellReadsInstances3 {

  protected val codecRegistry: CodecRegistry = CodecRegistry.DEFAULT

  implicit val udtValueCellReads: CellReads[UdtValue] =
    instance { (bytes, protocol, dataType) =>
      requireNonNull(bytes)
      CodecRegistry.DEFAULT.codecFor(dataType, classOf[UdtValue]).decode(bytes, protocol)
    }

  implicit def rawReadsFromUdtReads[T: UdtReads]: CellReads[T] =
    CellReads[UdtValue].map(UdtReads[T].read(_))

  def requireNonNull(bytes: ByteBuffer): Unit = if (bytes == null) throw UnexpectedNullValue.NullValueInColumn

}
