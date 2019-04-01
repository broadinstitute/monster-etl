package org.broadinstitute.monster.etl.encode.transforms

import io.circe.syntax._

/** Stream transformations run only on donor entities during ENCODE ETL. */
object DonorTransforms {

  /**
    * Break up the natural-text "phenotypes" field in Donor entities into an array.
    *
    * Splitting logic was decided upon by @danmoran after a quick scan through example
    * phenotype values. We should investigate better tooling to use instead.
    */
  def splitDonorPhenotypes: JsonPipe = _.transform("Split Donor Phenotypes") {
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
}
