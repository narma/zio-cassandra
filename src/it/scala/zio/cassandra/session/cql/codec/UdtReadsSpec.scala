package zio.cassandra.session.cql.codec

import zio.{ Chunk, ZIO }
import zio.cassandra.session.{ Session, ZIOCassandraSpec, ZIOCassandraSpecUtils }
import zio.test.Assertion.hasSameElements
import zio.test._

object UdtReadsSpec extends ZIOCassandraSpec with ZIOCassandraSpecUtils {

  final case class UdtTypeTestData(
    data: String,
    count: Option[Int],
    flag: Option[Boolean],
    dataset: Option[Set[Int]],
    datalist: Option[List[Int]],
    datamap: Option[Map[Int, String]]
  )

  final case class TypeTestData(id: BigInt, udt: UdtTypeTestData)

  final case class UdtNameTestData(ALLUPPER: String, alllower: String, someName: String, someLongName: String)

  final case class NameTestData(id: BigInt, udt: UdtNameTestData)

  private val nameTestData = NameTestData(0, UdtNameTestData("ALL-UPPER", "all-lower", "some-name", "some-long-name"))

  val spec: Spec[Session, Throwable] = suite("UdtReads")(
    test("should read simple data types") {
      val expected =
        Chunk(
          // cassandra can't differentiate null and List.empty / Map.empty, but can differentiate null and Set.empty
          TypeTestData(0, UdtTypeTestData("zero", None, None, None, None, None)),
          TypeTestData(1, UdtTypeTestData("one", None, None, Some(Set.empty), Some(List.empty), Some(Map.empty))),
          TypeTestData(
            2,
            UdtTypeTestData("two", Some(20), Some(false), Some(Set(200)), Some(List(210)), Some(Map(220 -> "2_zero")))
          ),
          TypeTestData(
            3,
            UdtTypeTestData(
              "three",
              Some(30),
              Some(true),
              Some(Set(300, 301, 302)),
              Some(List(310, 311, 312)),
              Some(Map(320 -> "3_zero", 321 -> "3_one"))
            )
          ),
          TypeTestData(4, UdtTypeTestData("four", None, None, None, None, None))
        )

      for {
        session <- ZIO.service[Session]
        result  <- session
                     .select(s"select id, udt FROM $keyspace.udt_reads_type_test")
                     .mapZIO(read[TypeTestData](_))
                     .runCollect
      } yield assert(result)(hasSameElements(expected))
    },
    test("should read names as is") {
      implicit val configuration: Configuration = Configuration(identity(_))

      for {
        session <- ZIO.service[Session]
        result  <- session
                     .selectFirst(
                       s"select id, udt FROM $keyspace.udt_reads_default_name_test"
                     )
                     .flatMap(readOpt[NameTestData](_))
      } yield assertTrue(result.contains(nameTestData))
    },
    test("should read names as is regardless of order in row") {
      implicit val configuration: Configuration = Configuration(identity(_))

      for {
        session <- ZIO.service[Session]
        result  <- session
                     .selectFirst(
                       s"select udt, id FROM $keyspace.udt_reads_default_name_test"
                     )
                     .flatMap(readOpt[NameTestData](_))
      } yield assertTrue(result.contains(nameTestData))
    },
    test("should read names transformed to snake_case") {
      for {
        session <- ZIO.service[Session]
        result  <- session
                     .selectFirst(
                       s"select id, udt FROM $keyspace.udt_reads_snake_name_test"
                     )
                     .flatMap(readOpt[NameTestData](_))
      } yield assertTrue(result.contains(nameTestData))
    },
    test("should read names transformed to snake_case regardless of order in row") {
      for {
        session <- ZIO.service[Session]
        result  <- session
                     .selectFirst(
                       s"select udt, id FROM $keyspace.udt_reads_snake_name_test"
                     )
                     .flatMap(readOpt[NameTestData](_))
      } yield assertTrue(result.contains(nameTestData))
    }
  )

}
