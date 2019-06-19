package org.broadinstitute.monster.etl.encode.transforms

import io.circe.Json
import io.circe.syntax._
import org.broadinstitute.monster.etl._
import org.broadinstitute.monster.etl.encode._

/** Stream transformations run only on donor entities during ENCODE ETL. */
object DonorTransforms {

  /** Extra ETL processing only for Donor entities from ENCODE. */
  def cleanDonors: JsonPipe =
    _.transform("Specialized Donor Prep") {
      List(splitPhenotypes, removeUnknowns, normalizeAges).reduce(_ andThen _)
    }

  /**
    * Break up the natural-text "phenotypes" field in Donor entities into an array.
    *
    * Splitting logic was decided upon by @danmoran after a quick scan through example
    * phenotype values. We should investigate better tooling to use instead.
    */
  private def splitPhenotypes: JsonPipe = _.transform("Split Donor Phenotypes") {
    _.map { json =>
      json("phenotype").flatMap(_.asString).fold(json.remove("phenotype")) {
        phenotypeString =>
          val splitPhenotypes = if (phenotypeString.contains(';')) {
            phenotypeString.split(';')
          } else if (phenotypeString.contains(',')) {
            phenotypeString.split(',')
          } else {
            Array(phenotypeString)
          }

          json.add("phenotype", splitPhenotypes.map(_.trim).asJson)
      }
    }
  }

  private val unknown = "unknown".asJson

  /**
    * Remove instances of 'unknown' from all donor JSON, since it's equivalent to no value.
    *
    * Especially important when writing to BigQuery because 'unknowns' will show up in
    * fields that should be numbers (i.e. age), violating the schema.
    */
  private def removeUnknowns: JsonPipe = _.transform("Remove Donor 'unknown's") {
    _.map { json =>
      json.filter {
        case (_, value) => value != unknown
      }.mapValues { value =>
        value.withArray(values => Json.fromValues(values.filterNot(_ == unknown)))
      }
    }
  }

  private val approxDaysPerMonth = 365.0 / 12.0

  /**
    * Normalize donor ages to remove ranges and reduce the number of possible units.
    *
    * We decided on these rules:
    *   1. Use separate columns for "min" and "max" ages to allow for ranges
    *   2. Convert any age less than 1 year to days, and anything above to years
    *   3. Remove age values for embryonic donors, since they can't be sensibly
    *      compared against post-birth donors.
    */
  private def normalizeAges: JsonPipe = _.transform("Normalize Donor Ages") {
    _.map { json =>
      val ageRangeAndUnit = for {
        rawAge <- json("age").flatMap(_.asString)
        unit <- json("age_units").flatMap(_.asString)
        // FIXME: Embryonic ages are pre-birth; we need to figure out how to represent
        // them so users don't search on them as if they were post-birth ages.
        _ <- json("life_stage")
          .flatMap(_.asString)
          .orElse(Some(""))
          .filter(_ != "embryonic")
      } yield {
        val splitIdx = rawAge.indexOf('-')
        val (baseMin, baseMax) = if (splitIdx < 0) {
          val asFloat = rawAge.toDouble
          (asFloat, asFloat)
        } else {
          val (stringMin, stringMax) = (rawAge.take(splitIdx), rawAge.drop(splitIdx + 1))
          (stringMin.toDouble, stringMax.toDouble)
        }

        val (min, max) = if (unit == "week") {
          (baseMin * 7.0, baseMax * 7.0)
        } else if (unit == "month") {
          (baseMin * approxDaysPerMonth, baseMax * approxDaysPerMonth)
        } else {
          (baseMin, baseMax)
        }

        if (unit != "year" && max > 365) {
          (min / 365.0, max / 365.0, "year")
        } else {
          (min, max, if (unit == "year") "year" else "day")
        }
      }

      ageRangeAndUnit
        .fold(json.remove("age_units")) {
          case (min, max, unit) =>
            json
              .add("min_age", min.asJson)
              .add("max_age", max.asJson)
              .add("age_units", unit.asJson)
        }
        .remove("age")
    }
  }
}
