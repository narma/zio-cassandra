package zio.cassandra.session.cql

import zio.cassandra.session.cql.codec.Configuration
import zio.test._

import scala.collection.mutable.ListBuffer

object ReadsConfigurationSpec extends ZIOSpecDefault {

  private val toSnakeCase = Configuration.snakeCaseTransformation

  override def spec: Spec[TestEnvironment, Any] =
    suite("Reads Configuration")(
      suite("snake_case field name transformer")(
        test("should not modify lower cased names") {
          val input    = "name"
          val expected = input
          assertTrue(toSnakeCase(input) == expected)
        },
        test("should not go crazy with uppper cased names") {
          val input    = "NAME"
          val expected = input.toLowerCase
          assertTrue(toSnakeCase(input) == expected)
        },
        test("should replace camelCase names with snakeCase") {
          val input    = "longName"
          val expected = "long_name"
          assertTrue(toSnakeCase(input) == expected)
        },
        test("should replace camelCase names with snakeCase if name is partially upper cased") {
          val input    = "IPProcessor"
          val expected = "ip_processor"
          assertTrue(toSnakeCase(input) == expected)
        }
      ),
      suite("NoTransformation")(
        test("should not change input string in any way") {
          val configuration = Configuration(Configuration.NoTransformation)
          checkN(32)(Gen.string) { str =>
            assertTrue(configuration.transformFieldNames(str) == str)
          }
        }
      ),
      suite("Transformation")(
        test("should change input string and re-evaluate result on each invocation") {
          val invoked             = new ListBuffer[String]()
          val f: String => String = {
            case "foo"  =>
              invoked.append("foo")
              "FOO"
            case "BAR"  =>
              invoked.append("bar")
              "bar"
            case "bUzz" =>
              invoked.append("buzz")
              "BuZZ"
            case other  =>
              invoked.append(other)
              other
          }

          val configuration = Configuration(Configuration.Transformation(f))

          configuration.transformFieldNames("foo")
          configuration.transformFieldNames("BAR")
          configuration.transformFieldNames("bUzz")
          configuration.transformFieldNames("other")
          configuration.transformFieldNames("other")
          configuration.transformFieldNames("bUzz")
          configuration.transformFieldNames("BAR")
          configuration.transformFieldNames("foo")

          assertTrue(invoked.toList == List("foo", "bar", "buzz", "other", "other", "buzz", "bar", "foo"))
        }
      ),
      suite("CachedTransformation")(
        test("should change input string and cache it") {
          val invoked             = new ListBuffer[String]()
          val f: String => String = {
            case "foo"  =>
              invoked.append("foo")
              "FOO"
            case "BAR"  =>
              invoked.append("bar")
              "bar"
            case "bUzz" =>
              invoked.append("buzz")
              "BuZZ"
            case other  =>
              invoked.append(other)
              other
          }

          val configuration = Configuration(Configuration.CachedTransformation(f))

          configuration.transformFieldNames("foo")
          configuration.transformFieldNames("BAR")
          configuration.transformFieldNames("bUzz")
          configuration.transformFieldNames("other")
          configuration.transformFieldNames("other")
          configuration.transformFieldNames("bUzz")
          configuration.transformFieldNames("BAR")
          configuration.transformFieldNames("foo")

          assertTrue(invoked.toList == List("foo", "bar", "buzz", "other"))
        }
      )
    )
}
