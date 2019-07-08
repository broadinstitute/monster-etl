package org.broadinstitute.monster.etl

import org.scalatest.{FlatSpec, Matchers}
import upack._

class MsgTransformationsSpec extends FlatSpec with Matchers {
  behavior of "MsgTransformations"

  it should "rename fields in object messages" in {
    val input = Obj(
      Str("foo") -> Str("bar"),
      Str("foobar") -> Int32(123),
      Str("baz") -> Arr(Str("qux"), Float64(1.23))
    )

    val renamed =
      MsgTransformations.renameFields(Map("baz" -> "wot", "foo" -> "wat"))(input)

    renamed shouldBe Obj(
      Str("wat") -> Str("bar"),
      Str("foobar") -> Int32(123),
      Str("wot") -> Arr(Str("qux"), Float64(1.23))
    )
  }

  it should "continue if a field-to-rename doesn't exist" in {
    val input = Obj(
      Str("foo") -> Str("bar"),
      Str("foobar") -> Int32(123),
      Str("baz") -> Arr(Str("qux"), Float64(1.23)),
      Str("abc") -> Int32(121)
    )

    val renamed =
      MsgTransformations.renameFields(Map("abc" -> "xyz", "lol" -> "haha"))(input)

    renamed shouldBe Obj(
      Str("foo") -> Str("bar"),
      Str("foobar") -> Int32(123),
      Str("baz") -> Arr(Str("qux"), Float64(1.23)),
      Str("xyz") -> Int32(121)
    )
  }

  it should "continue if none of the fields-to-rename exist" in {
    val input = Obj(
      Str("foo") -> Str("bar"),
      Str("foobar") -> Int32(123),
      Str("baz") -> Arr(Str("qux"), Float64(1.23))
    )

    val renamed =
      MsgTransformations.renameFields(Map("abc" -> "xyz", "lol" -> "haha"))(input)

    renamed shouldBe Obj(
      Str("foo") -> Str("bar"),
      Str("foobar") -> Int32(123),
      Str("baz") -> Arr(Str("qux"), Float64(1.23))
    )
  }

  it should "collect object fields into an array" in {
    ???
  }

  it should "preserve the order of input field names in the collected array" in {
    ???
  }

  it should "continue if a field-to-collect doesn't exist in a message" in {
    ???
  }

  it should "concatenate string fields into a single value" in {
    ???
  }

  it should "preserve the order of input field names in the concatenated string" in {
    ???
  }

  it should "fail if an expected field is missing during concatenation" in {
    val input = Obj(
      Str("foo") -> Str("bar"),
      Str("foobar") -> Int32(123),
      Str("baz") -> Arr(Str("qux"), Float64(1.23))
    )

    an[Exception] shouldBe thrownBy {
      MsgTransformations.concatFields(List("foo", "oops"), "combined", ":")(input)
    }
  }

  it should "convert all fields in an object message to snake-case" in {
    ???
  }

  it should "convert designated fields from strings to longs" in {
    ???
  }

  it should "strip zero-valued trailing decimal precision when converting to longs" in {
    ???
  }

  it should "fail to convert true floats/doubles to longs" in {
    ???
  }

  it should "support converting designated strings to 'nan' instead of longs" in {
    ???
  }

  it should "convert designated fields from strings to doubles" in {
    ???
  }

  it should "support converting designated strings to 'nan' instead of doubles" in {
    val input = Obj(
      Str("foo") -> Str("."),
      Str("bar") -> Str("98.7654"),
      Str("baz") -> Str(""),
      Str("notanum") -> Str("hello!"),
      Str("qux") -> Str("100")
    )

    val converted = MsgTransformations.parseDoubles(
      Set("foo", "bar", "baz", "qux"),
      nanValues = Set(".", "")
    )(input)

    converted shouldBe Obj(
      Str("foo") -> Str("nan"),
      Str("bar") -> Float64(98.7654),
      Str("baz") -> Str("nan"),
      Str("notanum") -> Str("hello!"),
      Str("qux") -> Float64(100.0)
    )
  }

  it should "convert designated fields from strings to booleans" in {
    ???
  }

  it should "support user-specified definitions of 'true' strings" in {
    ???
  }

  it should "convert designated fields from strings to arrays of strings" in {
    ???
  }

  it should "convert designated fields from strings to arrays of longs" in {
    ???
  }

  it should "convert designated fields from strings to arrays of doubles" in {
    ???
  }
}
