package org.broadinstitute.monster.etl

import cats.data.NonEmptyList
import upack.{Msg, Obj, Str}

import scala.collection.mutable

package object clinvar {

  /** Extension methods for interacting with upack Msgs. */
  implicit class MsgOps(val underlying: Msg) extends AnyVal {

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
        // NOTE: In the case when an XML tag contains both attributes and a text value,
        //  our extractor program assigns the text value to the '$' field. When attributes
        // are optional for a tag, this results in a mix of value types for that tag in
        // the output JSON (some objects w/ '$' fields, and some scalars).
        // We try to handle both cases by allowing drill-down paths that end in '$' to
        // terminate one hop early.
        case nonObj if fieldChain == NonEmptyList("$", Nil) =>
          logger.warn(s"Returning value $nonObj in place of '$$' field")
          Some(nonObj)
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
