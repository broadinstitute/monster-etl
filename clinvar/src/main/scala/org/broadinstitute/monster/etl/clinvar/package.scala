package org.broadinstitute.monster.etl

import cats.data.NonEmptyList
import upack.{Msg, Obj, Str}

import scala.collection.mutable

package object clinvar {

  /** Constant key used in Badger-Fish convention to store the scalar value of an XML tag. */
  val ValueKey: Msg = Str("$")

  /** Extension methods for interacting with upack Msgs. */
  implicit class MsgOps(val underlying: Msg) extends AnyVal {

    /**
      * Read the Badger-Fish value '$' from a Msg object.
      *
      * Will throw an exception if the wrapped Msg is not an object, or if
      * it is an object that doesn't contain the special-case key.
      */
    def value: Msg = underlying match {
      case Obj(fields) =>
        fields.getOrElse(
          ValueKey,
          throw new IllegalArgumentException(
            s"Key '$ValueKey' not found in object: $underlying"
          )
        )
      case _ =>
        throw new IllegalArgumentException(
          s"Cannot get value '$ValueKey' of non-object: $underlying'"
        )
    }

    /**
      * 'Drill' into a message, following a chain of fields, until either
      * the end of the chain is reached or an element of the chain is missing.
      *
      * @return the element at the end of the chain, if reachable
      */
    def extract(fieldChain: String*): Option[Msg] = {
      val logger = org.slf4j.LoggerFactory.getLogger(getClass)

      // Helper method that uses NonEmptyList for some extra guardrails.
      // NEL is convenient here but a pain to use elsewhere compared to the
      // var-args of the wrapping method.
      def drillDown(msg: Msg, fieldChain: NonEmptyList[String]): Option[Msg] = msg match {
        case Obj(fields) =>
          val firstKey = Str(fieldChain.head)
          NonEmptyList.fromList(fieldChain.tail) match {
            case None =>
              fields.remove(firstKey)
            case Some(remainingFields) =>
              fields.get(firstKey).flatMap { nested =>
                val retVal = drillDown(nested, remainingFields)
                nested match {
                  case Obj(nestedFields) if nestedFields.isEmpty =>
                    fields.remove(firstKey)
                    ()
                  case _ => ()
                }
                retVal
              }
          }
        case nonObj =>
          logger.warn(
            s"Attempted to extract field(s) [$fieldChain] from non-object: $nonObj"
          )
          None
      }

      NonEmptyList.fromList(fieldChain.toList).flatMap(drillDown(underlying, _))
    }

    /**
      * Pop and return a list of entries from a nested field within a message.
      *
      * XML sections with repeated tags are typically structured like:
      *
      *   <WrapperTag>
      *     <RepeatedTag></RepeatedTag>
      *     <RepeatedTag></RepeatedTag>
      *   </WrapperTag>
      *
      * Our XML->JSON converter munges things a little bit, by:
      *   1. Always typing `WrapperTag` as an object containing a single field named `RepeatedTag`
      *   2. Typing `RepeatedTag` as an array if multiple instances of the tag were present, and
      *      otherwise typing it as a non-array
      *
      * This extraction method assumes this conversion convention, descending through some wrapper
      * layer to pull out the maybe-array. If the nested value is not an array, it is wrapped by
      * an `Iterable` before being returned.
      *
      * @param wrapperName name of the tag / field which contains the potentially-repeated field
      * @param elementName name of the repeated tag / field which is nested within `wrapperName`
      * @return all values of `elementName` found under `wrapperName`
      */
    def extractList(wrapperName: String, elementName: String): mutable.ArrayBuffer[Msg] =
      underlying.obj
        .remove(Str(wrapperName))
        .fold(mutable.ArrayBuffer.empty[Msg])(
          MsgTransformations.popAsArray(_, elementName)
        )
  }
}
