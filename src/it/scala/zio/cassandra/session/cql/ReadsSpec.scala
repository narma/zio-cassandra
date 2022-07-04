package zio.cassandra.session.cql

import com.datastax.oss.driver.api.core.cql.{ Row, SimpleStatement }
import zio.cassandra.session.Session
import zio.test._
import zio.test.Assertion._
import zio._

object ReadsSpec {

  private val keyspace = "tests"

  implicit def toStatement(s: String): SimpleStatement = SimpleStatement.newInstance(s)

  final case class TypeTestData(
    id: BigInt,
    data: String,
    count: Option[Int],
    flag: Option[Boolean],
    dataset: Option[Set[Int]],
    datalist: List[Int],
    datamap: Map[Int, String])

  final case class NameTestData(id: BigInt, ALLUPPER: String, alllower: String, someName: String, someLongName: String)

  private val nameTestData = NameTestData(0, "ALL-UPPER", "all-lower", "some-name", "some-long-name")

  val readsTests = suite("Reads")(
    testM("should read simple data types") {
      val expected =
        Chunk(
          // cassandra can't differentiate null and List.empty / Map.empty, but can differentiate null and Set.empty
          TypeTestData(0, "zero", None, None, None, List.empty, Map.empty),
          TypeTestData(1, "one", None, None, Some(Set.empty), List.empty, Map.empty),
          TypeTestData(2, "two", Some(20), Some(false), Some(Set(200)), List(210), Map(220 -> "2_zero")),
          TypeTestData(
            3,
            "three",
            Some(30),
            Some(true),
            Some(Set(300, 301, 302)),
            List(310, 311, 312),
            Map(320 -> "3_zero", 321 -> "3_one")
          ),
          TypeTestData(4, "four", None, None, None, List.empty, Map.empty)
        )

      for {
        session <- ZIO.service[Session]
        result  <- session
                     .select(s"select id, data, count, flag, dataset, datalist, datamap FROM $keyspace.reads_type_test")
                     .mapM(read[TypeTestData](_))
                     .runCollect
      } yield assert(result)(hasSameElements(expected))
    },
    testM("should read names as is") {
      for {
        session <- ZIO.service[Session]
        result  <- session
                     .selectFirst(
                       s"select id, ALLUPPER, alllower, someName, someLongName FROM $keyspace.reads_default_name_test"
                     )
                     .flatMap(readOpt[NameTestData](_))
      } yield assertTrue(result.contains(nameTestData))
    },
    testM("should read names as is regardless of order in row") {
      for {
        session <- ZIO.service[Session]
        result  <- session
                     .selectFirst(
                       s"select alllower, id, someLongName, ALLUPPER, someName FROM $keyspace.reads_default_name_test"
                     )
                     .flatMap(readOpt[NameTestData](_))
      } yield assertTrue(result.contains(nameTestData))
    },
    testM("should read names transformed to snake_case") {
      implicit val configuration: Reads.Configuration = Reads.defaultConfiguration.withSnakeCase

      for {
        session <- ZIO.service[Session]
        result  <- session
                     .selectFirst(
                       s"select id, ALLUPPER, alllower, some_name, some_long_name FROM $keyspace.reads_snake_name_test"
                     )
                     .flatMap(readOpt[NameTestData](_))
      } yield assertTrue(result.contains(nameTestData))
    },
    testM("should read names transformed to snake_case regardless of order in row") {
      implicit val configuration: Reads.Configuration = Reads.defaultConfiguration.withSnakeCase

      for {
        session <- ZIO.service[Session]
        result  <- session
                     .selectFirst(
                       s"select alllower, id, some_long_name, ALLUPPER, some_name FROM $keyspace.reads_snake_name_test"
                     )
                     .flatMap(readOpt[NameTestData](_))
      } yield assertTrue(result.contains(nameTestData))
    }
  )

  private def read[T: Reads](row: Row): IO[TestFailure[Throwable], T] =
    Reads[T].read(row).mapError(TestFailure.die)

  private def readOpt[T: Reads](row: Option[Row]): IO[TestFailure[Throwable], Option[T]] =
    ZIO.foreach(row)(read[T](_))

}
