package zio.cassandra.session.cql

import com.datastax.oss.driver.api.core.`type`.UserDefinedType
import com.datastax.oss.driver.api.core.cql.BoundStatement
import com.datastax.oss.driver.api.core.data.UdtValue
import shapeless.{ ::, HList, HNil, Widen }

import java.nio.ByteBuffer
import java.time.{ Instant, LocalDate }
import java.util.UUID
import scala.annotation.implicitNotFound

@implicitNotFound("""Cannot find or construct a Binder instance for type:

  ${T}

  Construct it if needed, please refer to Binder source code for guidance
""")
trait Binder[T] { self =>
  def bind(statement: BoundStatement, index: Int, value: T): (BoundStatement, Int)

  def contramap[U](f: U => T): Binder[U] = new Binder[U] {
    override def bind(statement: BoundStatement, index: Int, value: U): (BoundStatement, Int) =
      self.bind(statement, index, f(value))
  }
}

object Binder extends BinderLowerPriority with BinderLowestPriority {

  def apply[T](implicit binder: Binder[T]): Binder[T] = binder

  implicit val stringBinder: Binder[String] = new Binder[String] {
    override def bind(statement: BoundStatement, index: Int, value: String): (BoundStatement, Int) =
      (statement.setString(index, value), index + 1)
  }

  implicit val doubleBinder: Binder[Double] = new Binder[Double] {
    override def bind(statement: BoundStatement, index: Int, value: Double): (BoundStatement, Int) =
      (statement.setDouble(index, value), index + 1)
  }

  implicit val intBinder: Binder[Int] = new Binder[Int] {
    override def bind(statement: BoundStatement, index: Int, value: Int): (BoundStatement, Int) =
      (statement.setInt(index, value), index + 1)
  }

  implicit val longBinder: Binder[Long] = new Binder[Long] {
    override def bind(statement: BoundStatement, index: Int, value: Long): (BoundStatement, Int) =
      (statement.setLong(index, value), index + 1)
  }

  implicit val byteBufferBinder: Binder[ByteBuffer] = new Binder[ByteBuffer] {
    override def bind(statement: BoundStatement, index: Int, value: ByteBuffer): (BoundStatement, Int) =
      (statement.setByteBuffer(index, value), index + 1)
  }

  implicit val localDateBinder: Binder[LocalDate] = new Binder[LocalDate] {
    override def bind(statement: BoundStatement, index: Int, value: LocalDate): (BoundStatement, Int) =
      (statement.setLocalDate(index, value), index + 1)
  }

  implicit val instantBinder: Binder[Instant] = new Binder[Instant] {
    override def bind(statement: BoundStatement, index: Int, value: Instant): (BoundStatement, Int) =
      (statement.setInstant(index, value), index + 1)
  }

  implicit val booleanBinder: Binder[Boolean] = new Binder[Boolean] {
    override def bind(statement: BoundStatement, index: Int, value: Boolean): (BoundStatement, Int) =
      (statement.setBoolean(index, value), index + 1)
  }

  implicit val uuidBinder: Binder[UUID] = new Binder[UUID] {
    override def bind(statement: BoundStatement, index: Int, value: UUID): (BoundStatement, Int) =
      (statement.setUuid(index, value), index + 1)
  }

  implicit val bigIntBinder: Binder[BigInt] = new Binder[BigInt] {
    override def bind(statement: BoundStatement, index: Int, value: BigInt): (BoundStatement, Int) =
      (statement.setBigInteger(index, value.bigInteger), index + 1)
  }

  implicit val bigDecimalBinder: Binder[BigDecimal] = new Binder[BigDecimal] {
    override def bind(statement: BoundStatement, index: Int, value: BigDecimal): (BoundStatement, Int) =
      (statement.setBigDecimal(index, value.bigDecimal), index + 1)
  }

  implicit val shortBinder: Binder[Short] = new Binder[Short] {
    override def bind(statement: BoundStatement, index: Int, value: Short): (BoundStatement, Int) =
      (statement.setShort(index, value), index + 1)
  }

  implicit val userDefinedTypeValueBinder: Binder[UdtValue] =
    (statement: BoundStatement, index: Int, value: UdtValue) => (statement.setUdtValue(index, value), index + 1)

  implicit def optionBinder[T: Binder]: Binder[Option[T]] = new Binder[Option[T]] {
    override def bind(statement: BoundStatement, index: Int, value: Option[T]): (BoundStatement, Int) = value match {
      case Some(x) => Binder[T].bind(statement, index, x)
      case None    => (statement.setToNull(index), index + 1)
    }
  }

  implicit def widenBinder[T: Binder, X <: T](implicit wd: Widen.Aux[X, T]): Binder[X] = new Binder[X] {
    override def bind(statement: BoundStatement, index: Int, value: X): (BoundStatement, Int) =
      Binder[T].bind(statement, index, wd.apply(value))
  }

  implicit class UdtValueBinderOps(udtBinder: Binder[UdtValue]) {

    /** This is necessary for UDT values as you are not allowed to safely create a UDT value, instead you use the
      * prepared statement's variable definitions to retrieve a UserDefinedType that can be used as a constructor for a
      * UdtValue
      *
      * @param f
      *   is a function that accepts the input value A along with a constructor that you use to build the UdtValue that
      *   gets sent to Cassandra
      * @tparam A
      * @return
      */
    def contramapUDT[A](f: (A, UserDefinedType) => UdtValue): Binder[A] = new Binder[A] {
      override def bind(statement: BoundStatement, index: Int, value: A): (BoundStatement, Int) = {
        val udtValue = f(
          value,
          statement.getPreparedStatement.getVariableDefinitions.get(index).getType.asInstanceOf[UserDefinedType]
        )
        udtBinder.bind(statement, index, udtValue)
      }
    }
  }
}

trait BinderLowerPriority {

  /** This typeclass instance is used to (inductively) derive datatypes that can have arbitrary amounts of nesting
    * @param ev
    *   is evidence that a typeclass instance of CassandraTypeMapper exists for A
    * @tparam A
    *   is the Scala datatype that needs to be written to Cassandra
    * @return
    */
  implicit def deriveBinderFromCassandraTypeMapper[A](implicit ev: CassandraTypeMapper[A]): Binder[A] =
    (statement: BoundStatement, index: Int, value: A) => {
      val datatype  = statement.getType(index)
      val cassandra = ev.toCassandra(value, datatype)
      (statement.set(index, cassandra, ev.classType), index + 1)
    }
}

trait BinderLowestPriority {
  implicit val hNilBinder: Binder[HNil]                                   = new Binder[HNil] {
    override def bind(statement: BoundStatement, index: Int, value: HNil): (BoundStatement, Int) = (statement, index)
  }
  implicit def hConsBinder[H: Binder, T <: HList: Binder]: Binder[H :: T] = new Binder[H :: T] {
    override def bind(statement: BoundStatement, index: Int, value: H :: T): (BoundStatement, Int) = {
      val (applied, nextIndex) = Binder[H].bind(statement, index, value.head)
      Binder[T].bind(applied, nextIndex, value.tail)
    }
  }
}
