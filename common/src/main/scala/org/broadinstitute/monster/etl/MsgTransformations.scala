package org.broadinstitute.monster.etl

import upack._

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

/**
  * Collection of generic methods which can manipulate upack messages.
  *
  * Most of the functionality required by our transformation pipelines
  * can be boiled down into this set of operations.
  *
  * NOTE: These methods all return transformed copies of their inputs.
  * They _must not_ mutate their input messages, because the Beam model
  * assumes immutability as an invariant and will become corrupted if
  * that invariant is violated at runtime. The local/direct runner will
  * verify that pipeline stages don't modify their inputs, so proper
  * functional testing should guard against this issue.
  */
object MsgTransformations {

  /**
    * Rewrite the keys of a message so that every "old" key in a mapping
    * is replaced by a corresponding "new" key.
    *
    * Values of the rewritten keys are unaffected by this transformation.
    * Keys not included in the rewrite map are left in the message as-is.
    *
    * If an "old" key given in the rewrite map is not present in the message,
    * the transformation continues (as opposed to throwing an exception).
    */
  def renameFields(fields: Map[String, String])(msg: Msg): Msg = {
    val toRet = upack.copy(msg)
    val underlying = toRet.obj
    fields.foreach {
      case (oldName, newName) =>
        underlying.remove(Str(oldName)) match {
          case None             => ()
          case Some(fieldValue) => underlying.update(Str(newName), fieldValue)
        }
    }
    toRet
  }

  /**
    * Aggregate the values of a list of fields into a single array field
    * with a string key.
    *
    * Values in the collected array will appear in the same order as their
    * corresponding field names in the input to this method. Keys not included
    * in the collection list are left in the message as-is.
    *
    * If a field-to-collect is missing from the message, the transformation
    * continues (as opposed to throwing an exception).
    */
  def collectFields(fields: List[String], collectedName: String)(msg: Msg): Msg = {
    val toRet = upack.copy(msg)
    val underlying = toRet.obj
    val buf = Arr()
    fields.foreach { field =>
      underlying.remove(Str(field)) match {
        case None             => ()
        case Some(fieldValue) => buf.value += fieldValue
      }
    }
    underlying.update(Str(collectedName), buf)
    toRet
  }

  /**
    * Aggregate the values of a list of fields into a single string field,
    * separated by a delimiter, with a string key.
    *
    * Aggregated values must be strings, and will appear in the same order
    * as their corresponding field names in the input to this method. Keys
    * not included in the concatenation list are left in the message as-is.
    *
    * If a field-to-concat is missing from the message, the transformation
    * will throw an exception.
    */
  def concatFields(
    fields: List[String],
    concatName: String,
    sep: String
  )(msg: Msg): Msg = {
    val toRet = upack.copy(msg)
    val underlying = toRet.obj
    val builder = ArrayBuffer[String]()
    fields.foreach { field =>
      underlying.remove(Str(field)) match {
        case None =>
          throw new Exception(s"Expected field $field not found in $msg")
        case Some(fieldValue) =>
          builder.append(fieldValue.str)
      }
    }
    underlying.update(Str(concatName), Str(builder.mkString(sep)))
    toRet
  }

  /**
    * Convert every key in an object message to snake-case.
    *
    * Our version of snake-case inserts underscores between
    * groups of numbers and groups of letters, in addition to
    * between upper- and lower-case letters.
    */
  def keysToSnakeCase(msg: Msg): Msg = {
    val toRet = Obj()
    msg.obj.foreach {
      case (k, v) =>
        val snakeCase = k.str
          .replace("-", "_")
          .replaceAll("([A-Z]+)([A-Z][a-z])", "$1_$2")
          .replaceAll("([a-z\\d])([A-Z])", "$1_$2")
          .replaceAll("([a-z])([\\d])", "$1_$2")
          .replaceAll("([\\d])([a-z])", "$1_$2")
          .toLowerCase

        toRet.value.update(Str(snakeCase), v)
    }
    toRet
  }

  /**
    * Replace the string value of each field in a set with the result
    * of applying a mapping function to the value.
    *
    * Keys not included in the mapping set are left in the message as-is.
    *
    * If a field in the mapping set is not present in the message, the
    * transformation will continue (as opposed to throwing an exception).
    */
  private def mapFieldValues(fields: Set[String], msg: Msg)(f: String => Msg): Msg = {
    val toRet = upack.copy(msg)
    val underlying = toRet.obj
    fields.foreach { field =>
      underlying.remove(Str(field)) match {
        case None => ()
        case Some(fieldValue) =>
          Try(f(fieldValue.str)) match {
            case Success(mapped) => underlying.update(Str(field), mapped)
            case Failure(err) =>
              throw new Exception(
                s"Failed to map value $fieldValue of field $field",
                err
              )
          }
      }
    }
    toRet
  }

  /**
    * Parse a string into a upack Int64 message (or the string "nan").
    *
    * String values which should be considered "nan" are left to the caller
    * to define.
    */
  private def parseLong(strVal: String, nanValues: Set[String]): Msg = {
    if (nanValues.contains(strVal)) {
      Str("nan")
    } else {
      // FIXME: There must be a more robust way to do this.
      val toParse = if (strVal.endsWith(".0")) {
        strVal.dropRight(2)
      } else {
        strVal
      }
      Int64(toParse.toLong)
    }
  }

  /**
    * Parse a string into a upack Float64 message (or the string "nan").
    *
    * String values which should be considered "nan" are left to the caller
    * to define.
    */
  private def parseDouble(strVal: String, nanValues: Set[String]): Msg = {
    if (nanValues.contains(strVal)) {
      Str("nan")
    } else {
      Float64(strVal.toDouble)
    }
  }

  /**
    * Parse a string into a upack Bool.
    *
    * String values which should be considered "true" are left to the caller
    * to define.
    */
  private def parseBoolean(strVal: String, trueValues: Set[String]): Msg =
    Bool(trueValues.contains(strVal))

  /**
    * Parse a string into a upack Arr by splitting on a caller-defined delimiter.
    *
    * After splitting, pieces of the former string field are mapped via a caller-provided
    * function, to allow for injecting nested parsing logic.
    */
  private def parseArray(
    strVal: String,
    delimiter: String,
    subParse: String => Msg
  ): Msg = {
    val elements = strVal.split(delimiter).map(subParse)
    Arr(elements.to[ArrayBuffer])
  }

  /**
    * Convert the values of a set of fields in a message to longs.
    *
    * By default, any string value which can't be parsed to a number will cause
    * this transformation to fail. Callers can provide a set of string
    * values known to represent "nan" as an escape hatch.
    */
  def parseLongs(
    fields: Set[String],
    nanValues: Set[String] = Set.empty
  )(msg: Msg): Msg = mapFieldValues(fields, msg)(parseLong(_, nanValues))

  /**
    * Convert the values of a set of fields in a message to doubles.
    *
    * By default, any string value which can't be parsed to a number will cause
    * this transformation to fail. Callers can provide a set of string
    * values known to represent "nan" as an escape hatch.
    */
  def parseDoubles(
    fields: Set[String],
    nanValues: Set[String] = Set.empty
  )(msg: Msg): Msg = mapFieldValues(fields, msg)(parseDouble(_, nanValues))

  /**
    * Convert the values of a set of fields in a message to booleans.
    *
    * By default, only the string "true" is considered true. Callers can
    * provide an alternate set of string values which should map to a true value.
    */
  def parseBooleans(
    fields: Set[String],
    trueValues: Set[String] = Set("true")
  )(msg: Msg): Msg = mapFieldValues(fields, msg)(parseBoolean(_, trueValues))

  /**
    * Convert the values of a set of fields in a message to string arrays.
    *
    * The delimiter used to split the string values must be the same across the given fields.
    */
  def parseStringArrays(fields: Set[String], delimiter: String)(msg: Msg): Msg =
    mapFieldValues(fields, msg)(parseArray(_, delimiter, Str))

  /**
    * Convert the values of a set of fields in a message to long arrays.
    *
    * The delimiter used to split the string values must be the same across the given fields.
    * Callers can provide a set of white-listed strings which will be converted to "nan"
    * instead of tripping an exception.
    */
  def parseLongArrays(
    fields: Set[String],
    delimiter: String,
    nanValues: Set[String] = Set.empty
  )(msg: Msg): Msg =
    mapFieldValues(fields, msg)(parseArray(_, delimiter, parseLong(_, nanValues)))

  /**
    * Convert the values of a set of fields in a message to double arrays.
    *
    * The delimiter used to split the string values must be the same across the given fields.
    * Callers can provide a set of white-listed strings which will be converted to "nan"
    * instead of tripping an exception.
    */
  def parseDoubleArrays(
    fields: Set[String],
    delimiter: String,
    nanValues: Set[String] = Set.empty
  )(msg: Msg): Msg =
    mapFieldValues(fields, msg)(parseArray(_, delimiter, parseDouble(_, nanValues)))
}
