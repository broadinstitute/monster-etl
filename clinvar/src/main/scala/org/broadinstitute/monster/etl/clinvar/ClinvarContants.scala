package org.broadinstitute.monster.etl.clinvar

import upack._

/** Container for constants that we use across the ClinVar pipeline. */
object ClinvarContants {

  //// Key names ////

  /** Column used as a primary key when no accession is present in a table. */
  val IdKey: Msg = Str("id")

  /** Standard name used for columns pointing to the VCV table. */
  val VcvRef: Msg = Str("variation_archive_id")

  /** Standard name used for columns pointing to the RCV table. */
  val RcvRef: Msg = Str("rcv_accession_id")

  /** Standard name used for columns pointing to the VCV Variation table. */
  val VarRef: Msg = Str("variation_id")

  /** Standard name used for columns pointing to the SCV table. */
  val ScvRef: Msg = Str("clinical_assertion_id")

  /** Standard name used for columns pointing to the Submitter table. */
  val SubmitterRef: Msg = Str("submitter_id")

  /** Standard name used for columns pointing to the Submission table. */
  val SubmissionRef: Msg = Str("submission_id")

  //// Fields generated when unrolling variants ////

  /** Standard name used in variant tables to point to the immediate children in the same table. */
  val ChildrenRef: Msg = Str("child_ids")

  /** Standard name used in variant tables to point to all descendants in the same table. */
  val DescendantsRef: Msg = Str("descendant_ids")

  /** Standard name used in variant tables to mark the type of variant. */
  val SubclassKey: Msg = Str("subclass_type")

  /** All fields generated while splitting ClinVar data. */
  val GeneratedKeys: Set[Msg] = Set(
    IdKey,
    VcvRef,
    RcvRef,
    VarRef,
    ScvRef,
    SubmitterRef,
    SubmissionRef,
    ChildrenRef,
    DescendantsRef,
    SubclassKey
  )

  //// Pseudo-enums found in the XML ////

  /** Type for "real" VCVs backed by submissions to ClinVar. */
  val InterpretedRecord: Msg = Str("InterpretedRecord")

  /**
    * Type for "fake" VCVs generated by ClinVar to model nested variations
    * which don't have their own top-level submissions.
    */
  val IncludedRecord: Msg = Str("IncludedRecord")

  /** Supported types of variants in VCVs and SCVs. */
  val VariationTypes: Set[Msg] = Set("SimpleAllele", "Haplotype", "Genotype").map(Str)
}
