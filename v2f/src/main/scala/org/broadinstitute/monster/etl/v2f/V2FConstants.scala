package org.broadinstitute.monster.etl.v2f

/**
  * Constants for V2F that allow for transforms of the converted TSV to Msg.
  */
sealed trait V2FConstants {

  /**
    * The name of the TSVs that are being converted to Msg and transformed.
    */
  def tableName: String

  /**
    * File pattern matching TSVs to process within the V2F analysis directory.
    */
  def filePath: String

  /**
    * The names of Msg fields that should be converted from Strings to Doubles.
    */
  def fieldsToConvertToMsgDouble: Set[String] = Set.empty

  /**
    * The names of Msg fields that should be converted from Strings to Longs.
    * Converting to Long instead due to Msg schema Long columns not accepting doubles
    */
  def fieldsToConvertToMsgLong: Set[String] = Set.empty

  /**
    * The names of Msg fields that should be converted from Strings to a Booleans.
    */
  def fieldsToConvertToMsgBoolean: Set[String] = Set.empty

  /**
    * The names of Msg fields that should be converted from from Strings to Arrays.
    * The keys of the map are delimiter and the values of map are names of Msg fields for the give delimiter(key).
    * e.g: "24,81,5,8,60" to ["24", "81", "5", "8", "60"]
    */
  def fieldsToConvertToStringArray: Map[String, Set[String]] = Map.empty

  /**
    * The names of Msg Arrays that should be converted from arrays Strings to arrays Doubles.
    * e.g: "24,81,5,8,60" to [24, 81, 5, 8, 60]
    */
  def fieldsToConvertToDoubleArray: Map[String, Set[String]] = Map.empty

  /**
    * The names of Msg fields that should be renamed.
    * e.g: "kobe": "Bryant" to "Kobe": "Bryant"
    */
  def fieldsToRename: Map[String, String] = Map.empty

  /**
    * The names of Msg fields that should be removed.
    */
  def fieldsToRemove: Set[String] = Set.empty

  /**
    * The names of Msg fields that should be extracted to there own Msg. This isn't actually needed for every table,
    * but the only ones that use this all have this same Set so it makes sense to write it only once.
    */
  def variantFieldsToExtract: Set[String] =
    Set("id", "chromosome", "position", "reference", "alt")

  /**
    * The names of Msg fields that should be renamed for variants. Same logic as the above constant.
    */
  def variantFieldsToRename: Map[String, String] = Map("var_id" -> "id")
}

case object FrequencyAnalysis extends V2FConstants {
  override def tableName = "Frequency Analysis"

  override def filePath: String = "frequency-analysis"

  override def fieldsToConvertToMsgDouble: Set[String] = Set(
    "eaf",
    "maf"
  )

  override def fieldsToConvertToMsgLong: Set[String] = Set(
    "position"
  )

  override def fieldsToRename: Map[String, String] = Map("var_id" -> "variant_id")

  override def fieldsToRemove: Set[String] =
    Set("chromosome", "position", "reference", "alt")
}

case object MetaAnalysisAncestrySpecific extends V2FConstants {
  override def tableName = "Ancestry-Specific Meta-Analysis"

  override def filePath: String = "meta-analysis/ancestry-specific"

  override def fieldsToConvertToMsgDouble: Set[String] = Set(
    "p_value",
    "beta",
    "std_err"
  )

  override def fieldsToConvertToMsgLong: Set[String] = Set(
    "n",
    "position"
  )

  override def fieldsToRename: Map[String, String] = Map("var_id" -> "variant_id")

  override def fieldsToRemove: Set[String] =
    Set("chromosome", "position", "reference", "alt")
}

case object MetaAnalysisTransEthnic extends V2FConstants {
  override def tableName = "Trans-Ethnic Meta-Analysis"

  override def filePath: String = "meta-analysis/trans-ethnic"

  override def fieldsToConvertToMsgDouble: Set[String] = Set(
    "p_value",
    "z_score",
    "std_err",
    "beta"
  )

  override def fieldsToConvertToMsgLong: Set[String] = Set(
    "n",
    "position"
  )

  override def fieldsToConvertToMsgBoolean: Set[String] = Set("top")

  override def fieldsToRename: Map[String, String] = Map("var_id" -> "variant_id")

  override def fieldsToRemove: Set[String] =
    Set("chromosome", "position", "reference", "alt")
}

case object VariantEffectRegulatoryFeatureConsequences extends V2FConstants {
  override def tableName = "Regulatory Feature Consequences"

  override def filePath: String = "variant-effect/regulatory-feature-consequences"

  override def fieldsToConvertToMsgBoolean: Set[String] = Set(
    "pick"
  )

  override def fieldsToConvertToStringArray: Map[String, Set[String]] = Map(
    "," -> Set("consequence_terms")
  )

  override def fieldsToRename: Map[String, String] = Map("id" -> "variant_id")
}

case object VariantEffectTranscriptConsequences extends V2FConstants {
  override def tableName = "Transcript Consequences"

  override def filePath: String = "variant-effect/transcript-consequences"

  override def fieldsToConvertToMsgDouble: Set[String] = Set(
    "cadd_phred",
    "cadd_raw",
    "cadd_raw_rankscore",
    "dann_rankscore",
    "dann_score",
    "eigen_pc_raw",
    "eigen_pc_raw_rankscore",
    "eigen_phred",
    "eigen_raw",
    "fathmm_converted_rankscore",
    "fathmm_mkl_coding_rankscore",
    "fathmm_mkl_coding_score",
    "genocanyon_score",
    "genocanyon_score_rankscore",
    "gerp_plus_plus_nr",
    "gerp_plus_plus_rs",
    "gerp_plus_plus_rs_rankscore",
    "gm_12878_confidence_value",
    "gm_12878_fitcons_score",
    "gm_12878_fitcons_score_rankscore",
    "h_1_hesc_confidence_value",
    "h_1_hesc_fitcons_score",
    "h_1_hesc_fitcons_score_rankscore",
    "huvec_confidence_value",
    "huvec_fitcons_score",
    "huvec_fitcons_score_rankscore",
    "integrated_confidence_value",
    "integrated_fitcons_score",
    "integrated_fitcons_score_rankscore",
    "lrt_converted_rankscore",
    "lrt_omega",
    "lrt_score",
    "metalr_rankscore",
    "metalr_score",
    "metasvm_rankscore",
    "metasvm_score",
    "mutationassessor_score",
    "mutationassessor_score_rankscore",
    "mutationtaster_converted_rankscore",
    "phastcons_100_way_vertebrate",
    "phastcons_100_way_vertebrate_rankscore",
    "phastcons_20_way_mammalian",
    "phastcons_20_way_mammalian_rankscore",
    "phylop_100_way_vertebrate",
    "phylop_100_way_vertebrate_rankscore",
    "phylop_20_way_mammalian",
    "phylop_20_way_mammalian_rankscore",
    "polyphen_2_hdiv_rankscore",
    "polyphen_2_hvar_rankscore",
    "polyphen_score",
    "provean_converted_rankscore",
    "sift_converted_rankscore",
    "siphy_29_way_logodds",
    "siphy_29_way_logodds_rankscore",
    "vest_3_rankscore"
  )

  override def fieldsToConvertToMsgLong: Set[String] = Set(
    "cdna_end",
    "cdna_start",
    "cds_end",
    "cds_start",
    "distance",
    "protein_end",
    "protein_start",
    "reliability_index",
    "strand"
  )

  override def fieldsToConvertToMsgBoolean: Set[String] = Set(
    "canonical",
    "pick"
  )

  override def fieldsToConvertToStringArray: Map[String, Set[String]] = Map(
    "," -> Set(
      "consequence_terms",
      "fathmm_pred",
      "flags",
      "lof_flags",
      "mutationtaster_aae",
      "mutationtaster_model",
      "mutationtaster_pred",
      "provean_pred",
      "sift_pred",
      "transcript_id_vest_3",
      "transcript_var_vest_3",
      "interpro_domain"
    )
  )

  override def fieldsToConvertToDoubleArray: Map[String, Set[String]] = Map(
    "," -> Set(
      "mutationtaster_score",
      "vest_3_score",
      "polyphen_2_hdiv_score",
      "polyphen_2_hvar_score",
      "sift_score",
      "fathmm_score",
      "provean_score"
    ),
    ":" -> Set(
      "siphy_29_way_pi"
    )
  )

  override def fieldsToRename: Map[String, String] = Map(
    "gerp++_nr" -> "gerp_plus_plus_nr",
    "gerp++_rs" -> "gerp_plus_plus_rs",
    "gerp++_rs_rankscore" -> "gerp_plus_plus_rs_rankscore",
    "id" -> "variant_id"
  )
}

case object DatasetSpecificAnalysis extends V2FConstants {
  override def tableName = "Dataset Specific Analysis"

  override def filePath: String = "dataset-specific"

  override def fieldsToRename: Map[String, String] = Map("var_id" -> "variant_id")

  override def fieldsToRemove: Set[String] = Set(
    "chromosome",
    "position",
    "reference",
    "alt"
  )
}
