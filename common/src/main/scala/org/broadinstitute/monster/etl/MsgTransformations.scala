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
    *
    * @param fields the map of old field names to new field names in the Msg object
    * @param msg the Msg object to transform
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
    * Remove the key-value pairs of a message for a given field.
    *
    * Keys not included in the removal are left in the message as-is.
    *
    * If a "remove" key given in fields is not present in the message,
    * the transformation continues (as opposed to throwing an exception).
    *
    * @param fields the set of field names to remove from the Msg object
    * @param msg the Msg object to transform
    */
  def removeFields(fields: Set[String])(msg: Msg): Msg = {
    val toRet = upack.copy(msg)
    val underlying = toRet.obj
    fields.foreach { field =>
      underlying.remove(Str(field))
    }
    toRet
  }

  /**
    * Extract the key-value pairs of a message for a given field.
    *
    * Keys not included in the fields input are left out.
    *
    * If none of the fields to extract are present in the message,
    * throw an exception.
    *
    * @param fields the set of field names to extract from the Msg object
    * @param msg the Msg object to transform
    */
  def extractFields(fields: Set[String])(msg: Msg): Msg = {
    val toRet = Obj()
    val underlying = toRet.obj
    fields.foreach { field =>
      msg.obj.get(Str(field)).foreach { value =>
        underlying.put(Str(field), value)
      }
    }
    if (toRet.obj.isEmpty) {
      throw new Exception(
        s"Failed to extract fields, none of the fields to extract are present: $toRet"
      )
    } else {
      toRet
    }
  }

  /**
    * Aggregate the values of a Set of fields into a single array field
    * with a string key.
    *
    * Values in the collected array will appear in the same order as their
    * corresponding field names in the input to this method. Keys not included
    * in the collection Set are left in the message as-is.
    *
    * If a field-to-collect is missing from the message, the transformation
    * continues (as opposed to throwing an exception).
    *
    * @param fields the list of field names to aggregate
    * @param collectedName the field name to aggregate fields into
    * @param msg the Msg object to transform
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
    * Aggregate the values of a Set of fields into a single string field,
    * separated by a delimiter, with a string key.
    *
    * Aggregated values must be strings, and will appear in the same order
    * as their corresponding field names in the input to this method. Keys
    * not included in the concatenation Set are left in the message as-is.
    *
    * If a field-to-concat is missing from the message, the transformation
    * will throw an exception.
    *
    * @param fields the list of field names to aggregate
    * @param concatName the field name to aggregate fields into
    * @param sep delimiter by which to separate the aggregated strings
    * @param msg the Msg object to transform
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

  /** TODO */
  def keyToSnakeCase(k: String): String =
    k.replace("-", "_")
      .replaceAll("([A-Z]+)([A-Z][a-z])", "$1_$2")
      .replaceAll("([a-z\\d])([A-Z])", "$1_$2")
      .replaceAll("([a-z])([\\d])", "$1_$2")
      .replaceAll("([\\d])([a-z])", "$1_$2")
      .toLowerCase

  /**
    * Convert every key in an object message to snake-case.
    *
    * Our version of snake-case inserts underscores between
    * groups of numbers and groups of letters, in addition to
    * between upper- and lower-case letters.
    *
    * @param msg the Msg object to transform
    */
  def keysToSnakeCase(msg: Msg): Msg = {
    val toRet = Obj()
    msg.obj.foreach {
      case (k, v) =>
        val snakeCase = Str(keyToSnakeCase(k.str))
        toRet.value.update(snakeCase, v)
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
    *
    * @param fields the set of field names with values to be mapped
    * @param msg the Msg object to transform
    * @param f the function to apply
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
    *
    * @param strVal the string to parse
    * @param nanValues a set defining what qualifies as nan
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
    *
    * @param strVal the string to parse
    * @param nanValues a set defining what qualifies as a nan
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
    *
    * @param strVal the string to parse
    * @param trueValues a set defining what qualifies as true
    */
  private def parseBoolean(strVal: String, trueValues: Set[String]): Msg =
    Bool(trueValues.contains(strVal))

  /**
    * Parse a string into a upack Arr by splitting on a caller-defined delimiter.
    *
    * After splitting, pieces of the former string field are mapped via a caller-provided
    * function, to allow for injecting nested parsing logic.
    *
    * @param strVal the string to parse
    * @param delimiter the delimiter to split the string into an array on
    * @param subParse a function defining how to map the newly formed array
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
    *
    * @param fields the set of field names to parse
    * @param nanValues a set defining what qualifies as nan
    * @param msg the Msg object to transform
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
    *
    * @param fields the set of field names to parse
    * @param nanValues a set defining what qualifies as nan
    * @param msg the Msg object to transform
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
    *
    * @param fields the set of field names to parse
    * @param trueValues a set defining what qualifies as true
    * @param msg the Msg object to transform
    */
  def parseBooleans(
    fields: Set[String],
    trueValues: Set[String] = Set("true")
  )(msg: Msg): Msg = mapFieldValues(fields, msg)(parseBoolean(_, trueValues))

  /**
    * Convert the values of a set of fields in a message to string arrays.
    *
    * The delimiter used to split the string values must be the same across the given fields.
    *
    * @param fields the set of fields to transform to string arrays
    * @param delimiter the delimiter used to split the string values
    * @param msg the Msg object to transform
    */
  def parseStringArrays(fields: Set[String], delimiter: String)(msg: Msg): Msg =
    mapFieldValues(fields, msg)(parseArray(_, delimiter, Str))

  /**
    * Convert the values of a set of fields in a message to long arrays.
    *
    * The delimiter used to split the string values must be the same across the given fields.
    * Callers can provide a set of white-listed strings which will be converted to "nan"
    * instead of tripping an exception.
    *
    * @param fields the set of fields to transform to string arrays
    * @param delimiter the delimiter used to split the string values
    * @param nanValues a set defining what qualifies as nan
    * @param msg the Msg object to transform
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
    *
    * @param fields the set of fields to transform to string arrays
    * @param delimiter the delimiter used to split the string values
    * @param nanValues a set defining what qualifies as nan
    * @param msg the Msg object to transform
    */
  def parseDoubleArrays(
    fields: Set[String],
    delimiter: String,
    nanValues: Set[String] = Set.empty
  )(msg: Msg): Msg =
    mapFieldValues(fields, msg)(parseArray(_, delimiter, parseDouble(_, nanValues)))

  def popAsArray(msg: Msg, field: String): ArrayBuffer[Msg] =
    msg.obj.remove(Str(field)) match {
      case None            => ArrayBuffer.empty
      case Some(Arr(msgs)) => msgs
      case Some(oneMsg)    => ArrayBuffer(oneMsg)
    }

  /**
    * Ensure that all the values for a set of fields are arrays.
    *
    * Fields that are already arrays are not modified by this method. Scalar fields
    * are converted to singleton arrays.
    *
    * @param fields the set of fields to ensure are arrays
    * @param msg the Msg object to transform
    */
  def ensureArrays(fields: Set[String])(msg: Msg): Msg = {
    val copy = upack.copy(msg)
    fields.foreach { field =>
      copy.obj.update(Str(field), Arr(popAsArray(msg, field)))
    }
    copy
  }
}
