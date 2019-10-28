package org.broadinstitute.monster.etl.clinvar

import upack._

/** Container for constants that we use across the ClinVar pipeline. */
object ClinvarConstants {

  /** Supported types of variants in VCVs and SCVs. */
  val VariationTypes: Set[Msg] = Set("SimpleAllele", "Haplotype", "Genotype").map(Str)

  /** Constant used to mark references to the MedGen database. */
  val MedGenKey: String = "MedGen"
}
