package zio.cassandra.session.cql.codec

import com.datastax.oss.driver.api.core.cql.Row
import shapeless._
import shapeless.labelled.{ field, FieldType }
import zio._
import zio.cassandra.session.cql.codec.Reads.instance

import scala.annotation.nowarn

/** The main typeclass for decoding Cassandra values, the only one that matters. <br>
  * [[zio.cassandra.session.cql.codec.RawReads]] and [[zio.cassandra.session.cql.codec.UdtReads]] are mostly an
  * implementation details. As long as you can provide and instance of [[zio.cassandra.session.cql.codec.Reads]] for
  * your class (regardless of how you've created it), everything should work just fine.
  */
trait Reads[T] {

  def read(row: Row): Task[T]

}

object Reads extends ReadsInstances1 {

  def apply[T](implicit reads: Reads[T]): Reads[T] = reads

  def instance[T](f: Row => Task[T]): Reads[T] = (row: Row) => f(row)

}

trait ReadsInstances1 extends ReadsInstances2 {

  implicit val rowReads: Reads[Row] = instance(ZIO.succeed(_))

  implicit def tuple1Reads[
    T1: RawReads
  ]: Reads[Tuple1[T1]] =
    instance { row =>
      for {
        t1  <- readByIndex[T1](row, 0)
      } yield Tuple1(t1)
    }

  implicit def tuple2Reads[
    T1: RawReads,
    T2: RawReads
  ]: Reads[(T1, T2)] =
    instance { row =>
      for {
        t1  <- readByIndex[T1](row, 0)
        t2  <- readByIndex[T2](row, 1)
      } yield (t1, t2)
    }

  implicit def tuple3Reads[
    T1: RawReads,
    T2: RawReads,
    T3: RawReads
  ]: Reads[(T1, T2, T3)] =
    instance { row =>
      for {
        t1  <- readByIndex[T1](row, 0)
        t2  <- readByIndex[T2](row, 1)
        t3  <- readByIndex[T3](row, 2)
      } yield (t1, t2, t3)
    }

  implicit def tuple4Reads[
    T1: RawReads,
    T2: RawReads,
    T3: RawReads,
    T4: RawReads
  ]: Reads[(T1, T2, T3, T4)] =
    instance { row =>
      for {
        t1  <- readByIndex[T1](row, 0)
        t2  <- readByIndex[T2](row, 1)
        t3  <- readByIndex[T3](row, 2)
        t4  <- readByIndex[T4](row, 3)
      } yield (t1, t2, t3, t4)
    }

  implicit def tuple5Reads[
    T1: RawReads,
    T2: RawReads,
    T3: RawReads,
    T4: RawReads,
    T5: RawReads
  ]: Reads[(T1, T2, T3, T4, T5)] =
    instance { row =>
      for {
        t1  <- readByIndex[T1](row, 0)
        t2  <- readByIndex[T2](row, 1)
        t3  <- readByIndex[T3](row, 2)
        t4  <- readByIndex[T4](row, 3)
        t5  <- readByIndex[T5](row, 4)
      } yield (t1, t2, t3, t4, t5)
    }

  implicit def tuple6Reads[
    T1: RawReads,
    T2: RawReads,
    T3: RawReads,
    T4: RawReads,
    T5: RawReads,
    T6: RawReads
  ]: Reads[(T1, T2, T3, T4, T5, T6)] =
    instance { row =>
      for {
        t1  <- readByIndex[T1](row, 0)
        t2  <- readByIndex[T2](row, 1)
        t3  <- readByIndex[T3](row, 2)
        t4  <- readByIndex[T4](row, 3)
        t5  <- readByIndex[T5](row, 4)
        t6  <- readByIndex[T6](row, 5)
      } yield (t1, t2, t3, t4, t5, t6)
    }

  implicit def tuple7Reads[
    T1: RawReads,
    T2: RawReads,
    T3: RawReads,
    T4: RawReads,
    T5: RawReads,
    T6: RawReads,
    T7: RawReads
  ]: Reads[(T1, T2, T3, T4, T5, T6, T7)] =
    instance { row =>
      for {
        t1  <- readByIndex[T1](row, 0)
        t2  <- readByIndex[T2](row, 1)
        t3  <- readByIndex[T3](row, 2)
        t4  <- readByIndex[T4](row, 3)
        t5  <- readByIndex[T5](row, 4)
        t6  <- readByIndex[T6](row, 5)
        t7  <- readByIndex[T7](row, 6)
      } yield (t1, t2, t3, t4, t5, t6, t7)
    }

  implicit def tuple8Reads[
    T1: RawReads,
    T2: RawReads,
    T3: RawReads,
    T4: RawReads,
    T5: RawReads,
    T6: RawReads,
    T7: RawReads,
    T8: RawReads
  ]: Reads[(T1, T2, T3, T4, T5, T6, T7, T8)] =
    instance { row =>
      for {
        t1  <- readByIndex[T1](row, 0)
        t2  <- readByIndex[T2](row, 1)
        t3  <- readByIndex[T3](row, 2)
        t4  <- readByIndex[T4](row, 3)
        t5  <- readByIndex[T5](row, 4)
        t6  <- readByIndex[T6](row, 5)
        t7  <- readByIndex[T7](row, 6)
        t8  <- readByIndex[T8](row, 7)
      } yield (t1, t2, t3, t4, t5, t6, t7, t8)
    }

  implicit def tuple9Reads[
    T1: RawReads,
    T2: RawReads,
    T3: RawReads,
    T4: RawReads,
    T5: RawReads,
    T6: RawReads,
    T7: RawReads,
    T8: RawReads,
    T9: RawReads
  ]: Reads[(T1, T2, T3, T4, T5, T6, T7, T8, T9)] =
    instance { row =>
      for {
        t1  <- readByIndex[T1](row, 0)
        t2  <- readByIndex[T2](row, 1)
        t3  <- readByIndex[T3](row, 2)
        t4  <- readByIndex[T4](row, 3)
        t5  <- readByIndex[T5](row, 4)
        t6  <- readByIndex[T6](row, 5)
        t7  <- readByIndex[T7](row, 6)
        t8  <- readByIndex[T8](row, 7)
        t9  <- readByIndex[T9](row, 8)
      } yield (t1, t2, t3, t4, t5, t6, t7, t8, t9)
    }

  implicit def tuple10Reads[
    T1: RawReads,
    T2: RawReads,
    T3: RawReads,
    T4: RawReads,
    T5: RawReads,
    T6: RawReads,
    T7: RawReads,
    T8: RawReads,
    T9: RawReads,
    T10: RawReads
  ]: Reads[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10)] =
    instance { row =>
      for {
        t1  <- readByIndex[T1](row, 0)
        t2  <- readByIndex[T2](row, 1)
        t3  <- readByIndex[T3](row, 2)
        t4  <- readByIndex[T4](row, 3)
        t5  <- readByIndex[T5](row, 4)
        t6  <- readByIndex[T6](row, 5)
        t7  <- readByIndex[T7](row, 6)
        t8  <- readByIndex[T8](row, 7)
        t9  <- readByIndex[T9](row, 8)
        t10 <- readByIndex[T10](row, 9)
      } yield (t1, t2, t3, t4, t5, t6, t7, t8, t9, t10)
    }

  implicit def tuple11Reads[
    T1: RawReads,
    T2: RawReads,
    T3: RawReads,
    T4: RawReads,
    T5: RawReads,
    T6: RawReads,
    T7: RawReads,
    T8: RawReads,
    T9: RawReads,
    T10: RawReads,
    T11: RawReads
  ]: Reads[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11)] =
    instance { row =>
      for {
        t1  <- readByIndex[T1](row, 0)
        t2  <- readByIndex[T2](row, 1)
        t3  <- readByIndex[T3](row, 2)
        t4  <- readByIndex[T4](row, 3)
        t5  <- readByIndex[T5](row, 4)
        t6  <- readByIndex[T6](row, 5)
        t7  <- readByIndex[T7](row, 6)
        t8  <- readByIndex[T8](row, 7)
        t9  <- readByIndex[T9](row, 8)
        t10 <- readByIndex[T10](row, 9)
        t11 <- readByIndex[T11](row, 10)
      } yield (t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11)
    }

  implicit def tuple12Reads[
    T1: RawReads,
    T2: RawReads,
    T3: RawReads,
    T4: RawReads,
    T5: RawReads,
    T6: RawReads,
    T7: RawReads,
    T8: RawReads,
    T9: RawReads,
    T10: RawReads,
    T11: RawReads,
    T12: RawReads
  ]: Reads[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12)] =
    instance { row =>
      for {
        t1  <- readByIndex[T1](row, 0)
        t2  <- readByIndex[T2](row, 1)
        t3  <- readByIndex[T3](row, 2)
        t4  <- readByIndex[T4](row, 3)
        t5  <- readByIndex[T5](row, 4)
        t6  <- readByIndex[T6](row, 5)
        t7  <- readByIndex[T7](row, 6)
        t8  <- readByIndex[T8](row, 7)
        t9  <- readByIndex[T9](row, 8)
        t10 <- readByIndex[T10](row, 9)
        t11 <- readByIndex[T11](row, 10)
        t12 <- readByIndex[T12](row, 11)
      } yield (t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12)
    }

  implicit def tuple13Reads[
    T1: RawReads,
    T2: RawReads,
    T3: RawReads,
    T4: RawReads,
    T5: RawReads,
    T6: RawReads,
    T7: RawReads,
    T8: RawReads,
    T9: RawReads,
    T10: RawReads,
    T11: RawReads,
    T12: RawReads,
    T13: RawReads
  ]: Reads[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13)] =
    instance { row =>
      for {
        t1  <- readByIndex[T1](row, 0)
        t2  <- readByIndex[T2](row, 1)
        t3  <- readByIndex[T3](row, 2)
        t4  <- readByIndex[T4](row, 3)
        t5  <- readByIndex[T5](row, 4)
        t6  <- readByIndex[T6](row, 5)
        t7  <- readByIndex[T7](row, 6)
        t8  <- readByIndex[T8](row, 7)
        t9  <- readByIndex[T9](row, 8)
        t10 <- readByIndex[T10](row, 9)
        t11 <- readByIndex[T11](row, 10)
        t12 <- readByIndex[T12](row, 11)
        t13 <- readByIndex[T13](row, 12)
      } yield (t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13)
    }

  implicit def tuple14Reads[
    T1: RawReads,
    T2: RawReads,
    T3: RawReads,
    T4: RawReads,
    T5: RawReads,
    T6: RawReads,
    T7: RawReads,
    T8: RawReads,
    T9: RawReads,
    T10: RawReads,
    T11: RawReads,
    T12: RawReads,
    T13: RawReads,
    T14: RawReads
  ]: Reads[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14)] =
    instance { row =>
      for {
        t1  <- readByIndex[T1](row, 0)
        t2  <- readByIndex[T2](row, 1)
        t3  <- readByIndex[T3](row, 2)
        t4  <- readByIndex[T4](row, 3)
        t5  <- readByIndex[T5](row, 4)
        t6  <- readByIndex[T6](row, 5)
        t7  <- readByIndex[T7](row, 6)
        t8  <- readByIndex[T8](row, 7)
        t9  <- readByIndex[T9](row, 8)
        t10 <- readByIndex[T10](row, 9)
        t11 <- readByIndex[T11](row, 10)
        t12 <- readByIndex[T12](row, 11)
        t13 <- readByIndex[T13](row, 12)
        t14 <- readByIndex[T14](row, 13)
      } yield (t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14)
    }

  implicit def tuple15Reads[
    T1: RawReads,
    T2: RawReads,
    T3: RawReads,
    T4: RawReads,
    T5: RawReads,
    T6: RawReads,
    T7: RawReads,
    T8: RawReads,
    T9: RawReads,
    T10: RawReads,
    T11: RawReads,
    T12: RawReads,
    T13: RawReads,
    T14: RawReads,
    T15: RawReads
  ]: Reads[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15)] =
    instance { row =>
      for {
        t1  <- readByIndex[T1](row, 0)
        t2  <- readByIndex[T2](row, 1)
        t3  <- readByIndex[T3](row, 2)
        t4  <- readByIndex[T4](row, 3)
        t5  <- readByIndex[T5](row, 4)
        t6  <- readByIndex[T6](row, 5)
        t7  <- readByIndex[T7](row, 6)
        t8  <- readByIndex[T8](row, 7)
        t9  <- readByIndex[T9](row, 8)
        t10 <- readByIndex[T10](row, 9)
        t11 <- readByIndex[T11](row, 10)
        t12 <- readByIndex[T12](row, 11)
        t13 <- readByIndex[T13](row, 12)
        t14 <- readByIndex[T14](row, 13)
        t15 <- readByIndex[T15](row, 14)
      } yield (t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14, t15)
    }

  implicit def tuple16Reads[
    T1: RawReads,
    T2: RawReads,
    T3: RawReads,
    T4: RawReads,
    T5: RawReads,
    T6: RawReads,
    T7: RawReads,
    T8: RawReads,
    T9: RawReads,
    T10: RawReads,
    T11: RawReads,
    T12: RawReads,
    T13: RawReads,
    T14: RawReads,
    T15: RawReads,
    T16: RawReads
  ]: Reads[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16)] =
    instance { row =>
      for {
        t1  <- readByIndex[T1](row, 0)
        t2  <- readByIndex[T2](row, 1)
        t3  <- readByIndex[T3](row, 2)
        t4  <- readByIndex[T4](row, 3)
        t5  <- readByIndex[T5](row, 4)
        t6  <- readByIndex[T6](row, 5)
        t7  <- readByIndex[T7](row, 6)
        t8  <- readByIndex[T8](row, 7)
        t9  <- readByIndex[T9](row, 8)
        t10 <- readByIndex[T10](row, 9)
        t11 <- readByIndex[T11](row, 10)
        t12 <- readByIndex[T12](row, 11)
        t13 <- readByIndex[T13](row, 12)
        t14 <- readByIndex[T14](row, 13)
        t15 <- readByIndex[T15](row, 14)
        t16 <- readByIndex[T16](row, 15)
      } yield (t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14, t15, t16)
    }

  implicit def tuple17Reads[
    T1: RawReads,
    T2: RawReads,
    T3: RawReads,
    T4: RawReads,
    T5: RawReads,
    T6: RawReads,
    T7: RawReads,
    T8: RawReads,
    T9: RawReads,
    T10: RawReads,
    T11: RawReads,
    T12: RawReads,
    T13: RawReads,
    T14: RawReads,
    T15: RawReads,
    T16: RawReads,
    T17: RawReads
  ]: Reads[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17)] =
    instance { row =>
      for {
        t1  <- readByIndex[T1](row, 0)
        t2  <- readByIndex[T2](row, 1)
        t3  <- readByIndex[T3](row, 2)
        t4  <- readByIndex[T4](row, 3)
        t5  <- readByIndex[T5](row, 4)
        t6  <- readByIndex[T6](row, 5)
        t7  <- readByIndex[T7](row, 6)
        t8  <- readByIndex[T8](row, 7)
        t9  <- readByIndex[T9](row, 8)
        t10 <- readByIndex[T10](row, 9)
        t11 <- readByIndex[T11](row, 10)
        t12 <- readByIndex[T12](row, 11)
        t13 <- readByIndex[T13](row, 12)
        t14 <- readByIndex[T14](row, 13)
        t15 <- readByIndex[T15](row, 14)
        t16 <- readByIndex[T16](row, 15)
        t17 <- readByIndex[T17](row, 16)
      } yield (t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14, t15, t16, t17)
    }

  implicit def tuple18Reads[
    T1: RawReads,
    T2: RawReads,
    T3: RawReads,
    T4: RawReads,
    T5: RawReads,
    T6: RawReads,
    T7: RawReads,
    T8: RawReads,
    T9: RawReads,
    T10: RawReads,
    T11: RawReads,
    T12: RawReads,
    T13: RawReads,
    T14: RawReads,
    T15: RawReads,
    T16: RawReads,
    T17: RawReads,
    T18: RawReads
  ]: Reads[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18)] =
    instance { row =>
      for {
        t1  <- readByIndex[T1](row, 0)
        t2  <- readByIndex[T2](row, 1)
        t3  <- readByIndex[T3](row, 2)
        t4  <- readByIndex[T4](row, 3)
        t5  <- readByIndex[T5](row, 4)
        t6  <- readByIndex[T6](row, 5)
        t7  <- readByIndex[T7](row, 6)
        t8  <- readByIndex[T8](row, 7)
        t9  <- readByIndex[T9](row, 8)
        t10 <- readByIndex[T10](row, 9)
        t11 <- readByIndex[T11](row, 10)
        t12 <- readByIndex[T12](row, 11)
        t13 <- readByIndex[T13](row, 12)
        t14 <- readByIndex[T14](row, 13)
        t15 <- readByIndex[T15](row, 14)
        t16 <- readByIndex[T16](row, 15)
        t17 <- readByIndex[T17](row, 16)
        t18 <- readByIndex[T18](row, 17)
      } yield (t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14, t15, t16, t17, t18)
    }

  implicit def tuple19Reads[
    T1: RawReads,
    T2: RawReads,
    T3: RawReads,
    T4: RawReads,
    T5: RawReads,
    T6: RawReads,
    T7: RawReads,
    T8: RawReads,
    T9: RawReads,
    T10: RawReads,
    T11: RawReads,
    T12: RawReads,
    T13: RawReads,
    T14: RawReads,
    T15: RawReads,
    T16: RawReads,
    T17: RawReads,
    T18: RawReads,
    T19: RawReads
  ]: Reads[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19)] =
    instance { row =>
      for {
        t1  <- readByIndex[T1](row, 0)
        t2  <- readByIndex[T2](row, 1)
        t3  <- readByIndex[T3](row, 2)
        t4  <- readByIndex[T4](row, 3)
        t5  <- readByIndex[T5](row, 4)
        t6  <- readByIndex[T6](row, 5)
        t7  <- readByIndex[T7](row, 6)
        t8  <- readByIndex[T8](row, 7)
        t9  <- readByIndex[T9](row, 8)
        t10 <- readByIndex[T10](row, 9)
        t11 <- readByIndex[T11](row, 10)
        t12 <- readByIndex[T12](row, 11)
        t13 <- readByIndex[T13](row, 12)
        t14 <- readByIndex[T14](row, 13)
        t15 <- readByIndex[T15](row, 14)
        t16 <- readByIndex[T16](row, 15)
        t17 <- readByIndex[T17](row, 16)
        t18 <- readByIndex[T18](row, 17)
        t19 <- readByIndex[T19](row, 18)
      } yield (t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14, t15, t16, t17, t18, t19)
    }

  implicit def tuple20Reads[
    T1: RawReads,
    T2: RawReads,
    T3: RawReads,
    T4: RawReads,
    T5: RawReads,
    T6: RawReads,
    T7: RawReads,
    T8: RawReads,
    T9: RawReads,
    T10: RawReads,
    T11: RawReads,
    T12: RawReads,
    T13: RawReads,
    T14: RawReads,
    T15: RawReads,
    T16: RawReads,
    T17: RawReads,
    T18: RawReads,
    T19: RawReads,
    T20: RawReads
  ]: Reads[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20)] =
    instance { row =>
      for {
        t1  <- readByIndex[T1](row, 0)
        t2  <- readByIndex[T2](row, 1)
        t3  <- readByIndex[T3](row, 2)
        t4  <- readByIndex[T4](row, 3)
        t5  <- readByIndex[T5](row, 4)
        t6  <- readByIndex[T6](row, 5)
        t7  <- readByIndex[T7](row, 6)
        t8  <- readByIndex[T8](row, 7)
        t9  <- readByIndex[T9](row, 8)
        t10 <- readByIndex[T10](row, 9)
        t11 <- readByIndex[T11](row, 10)
        t12 <- readByIndex[T12](row, 11)
        t13 <- readByIndex[T13](row, 12)
        t14 <- readByIndex[T14](row, 13)
        t15 <- readByIndex[T15](row, 14)
        t16 <- readByIndex[T16](row, 15)
        t17 <- readByIndex[T17](row, 16)
        t18 <- readByIndex[T18](row, 17)
        t19 <- readByIndex[T19](row, 18)
        t20 <- readByIndex[T20](row, 19)
      } yield (t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14, t15, t16, t17, t18, t19, t20)
    }

  implicit def tuple21Reads[
    T1: RawReads,
    T2: RawReads,
    T3: RawReads,
    T4: RawReads,
    T5: RawReads,
    T6: RawReads,
    T7: RawReads,
    T8: RawReads,
    T9: RawReads,
    T10: RawReads,
    T11: RawReads,
    T12: RawReads,
    T13: RawReads,
    T14: RawReads,
    T15: RawReads,
    T16: RawReads,
    T17: RawReads,
    T18: RawReads,
    T19: RawReads,
    T20: RawReads,
    T21: RawReads
  ]: Reads[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21)] =
    instance { row =>
      for {
        t1  <- readByIndex[T1](row, 0)
        t2  <- readByIndex[T2](row, 1)
        t3  <- readByIndex[T3](row, 2)
        t4  <- readByIndex[T4](row, 3)
        t5  <- readByIndex[T5](row, 4)
        t6  <- readByIndex[T6](row, 5)
        t7  <- readByIndex[T7](row, 6)
        t8  <- readByIndex[T8](row, 7)
        t9  <- readByIndex[T9](row, 8)
        t10 <- readByIndex[T10](row, 9)
        t11 <- readByIndex[T11](row, 10)
        t12 <- readByIndex[T12](row, 11)
        t13 <- readByIndex[T13](row, 12)
        t14 <- readByIndex[T14](row, 13)
        t15 <- readByIndex[T15](row, 14)
        t16 <- readByIndex[T16](row, 15)
        t17 <- readByIndex[T17](row, 16)
        t18 <- readByIndex[T18](row, 17)
        t19 <- readByIndex[T19](row, 18)
        t20 <- readByIndex[T20](row, 19)
        t21 <- readByIndex[T21](row, 20)
      } yield (t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14, t15, t16, t17, t18, t19, t20, t21)
    }

  implicit def tuple22Reads[
    T1: RawReads,
    T2: RawReads,
    T3: RawReads,
    T4: RawReads,
    T5: RawReads,
    T6: RawReads,
    T7: RawReads,
    T8: RawReads,
    T9: RawReads,
    T10: RawReads,
    T11: RawReads,
    T12: RawReads,
    T13: RawReads,
    T14: RawReads,
    T15: RawReads,
    T16: RawReads,
    T17: RawReads,
    T18: RawReads,
    T19: RawReads,
    T20: RawReads,
    T21: RawReads,
    T22: RawReads
  ]: Reads[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22)] =
    instance { row =>
      for {
        t1  <- readByIndex[T1](row, 0)
        t2  <- readByIndex[T2](row, 1)
        t3  <- readByIndex[T3](row, 2)
        t4  <- readByIndex[T4](row, 3)
        t5  <- readByIndex[T5](row, 4)
        t6  <- readByIndex[T6](row, 5)
        t7  <- readByIndex[T7](row, 6)
        t8  <- readByIndex[T8](row, 7)
        t9  <- readByIndex[T9](row, 8)
        t10 <- readByIndex[T10](row, 9)
        t11 <- readByIndex[T11](row, 10)
        t12 <- readByIndex[T12](row, 11)
        t13 <- readByIndex[T13](row, 12)
        t14 <- readByIndex[T14](row, 13)
        t15 <- readByIndex[T15](row, 14)
        t16 <- readByIndex[T16](row, 15)
        t17 <- readByIndex[T17](row, 16)
        t18 <- readByIndex[T18](row, 17)
        t19 <- readByIndex[T19](row, 18)
        t20 <- readByIndex[T20](row, 19)
        t21 <- readByIndex[T21](row, 20)
        t22 <- readByIndex[T22](row, 21)
      } yield (t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14, t15, t16, t17, t18, t19, t20, t21, t22)
    }

}

trait ReadsInstances2 extends ReadsInstances3 {

  implicit val hNilReads: Reads[HNil] = instance(_ => ZIO.succeed(HNil))

  implicit def hConsReads[K <: Symbol, H, T <: HList](implicit
    configuration: Configuration,
    hReads: RawReads[H],
    tReads: Reads[T],
    fieldNameW: Witness.Aux[K]
  ): Reads[FieldType[K, H] :: T] =
    instance { row =>
      for {
        fieldName <- ZIO.succeed(configuration.transformFieldNames(fieldNameW.value.name))
        head      <- hReads
                       .read(row.getBytesUnsafe(fieldName), row.protocolVersion(), row.getType(fieldName))
                       .mapError(toThrowable(_, row, fieldName))
        tail      <- tReads.read(row)
      } yield field[K](head) :: tail
    }

  implicit def genericReads[T, Repr](implicit
    // compiler is lying, this configuration is actually used for Reads[Repr] derivation
    @nowarn("msg=never used") configuration: Configuration,
    gen: LabelledGeneric.Aux[T, Repr],
    reads: Reads[Repr]
  ): Reads[T] =
    instance(row => reads.read(row).map(gen.from))

  private def toThrowable(error: RawReads.Error, row: Row, fieldName: String): Throwable =
    error match {
      case RawReads.UnexpectedNullError  => UnexpectedNullValueInColumn(row, fieldName)
      case RawReads.InternalError(cause) => cause
    }

}

trait ReadsInstances3 {

  implicit def readsFromRawReads[T: RawReads]: Reads[T] = instance(readByIndex(_, 0))

  protected def readByIndex[T: RawReads](row: Row, index: Int): Task[T] =
    RawReads[T].read(row.getBytesUnsafe(index), row.protocolVersion(), row.getType(index)).mapError {
      case RawReads.UnexpectedNullError  => UnexpectedNullValueInColumn(row, index)
      case RawReads.InternalError(cause) => cause
    }

}
