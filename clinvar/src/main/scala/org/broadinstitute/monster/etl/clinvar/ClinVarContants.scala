package org.broadinstitute.monster.etl.clinvar

import upack._

/** Container for constants that we use across the ClinVar pipeline. */
object ClinVarContants {

  //// Key names ////

  /** Column used as a primary key when no accession is present in a table. */
  val IdKey: Msg = Str("id")

  /** Standard name used for columns pointing to the VCV table. */
  val VcvRef: Msg = Str("variation_archive_accession")

  /** Standard name used for columns pointing to the RCV table. */
  val RcvRef: Msg = Str("rcv_accessions")

  /** Standard name used for columns pointing to the VCV Variation table. */
  val VarRef: Msg = Str("variation_archive_variation_id")

  /** Standard name used for columns pointing to the SCV table. */
  val ScvRef: Msg = Str("clinical_assertion_accession")

  /** Standard name used for columns pointing to the Submitter table. */
  val SubmitterRef: Msg = Str("submitter_id")

  /** Standard name used for columns pointing to the Submission table. */
  val SubmissionRef: Msg = Str("submission_id")

  //// Fields generated when unrolling variants ////

  /** Standard name used in variant tables to point to the immediate parent in the same table. */
  val ParentRef: Msg = Str("parent_id")

  /** Standard name used in variant tables to point to all ancestors in the same table. */
  val ParentsRef: Msg = Str("parent_ids")

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
    ParentRef,
    ParentsRef,
    SubclassKey
  )

  //// Pseudo-enums found in the XML ////

  /** Supported types of top-level records in a VCV. */
  val RecordTypes: Set[Msg] = Set("InterpretedRecord", "IncludedRecord").map(Str)

  /** Supported types of variants in VCVs and SCVs. */
  val VariantTypes: Set[Msg] = Set("SimpleAllele", "Haplotype", "Genotype").map(Str)
}
