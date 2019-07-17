package org.broadinstitute.monster.etl.v2f

/**
  * Constants for V2F that allow for transforms of the converted TSV to JSON.
  */
sealed trait V2FConstants {

  /**
    * The name of the TSVs that are being converted to JSON and transformed.
    */
  def tableName: String

  /**
    * File pattern matching TSVs to process within the V2F analysis directory.
    */
  def filePath: String

  /**
    * The names of JSON fields that should be converted from Strings to Doubles.
    */
  def fieldsToConvertToJsonDouble: Set[String]

  /**
    * The names of JSON fields that should be converted from Strings to Longs.
    * Converting to Long instead due to JSON schema Long columns not accepting doubles
    */
  def fieldsToConvertToJsonLong: Set[String]

  /**
    * The names of JSON fields that should be converted from Strings to a Booleans.
    */
  def fieldsToConvertToJsonBoolean: Set[String]

  /**
    * The names of JSON fields that should be converted from from Strings to Arrays.
    * The keys of the map are delimiter and the values of map are names of JSON fields for the give delimiter(key).
    * e.g: "24,81,5,8,60" to ["24", "81", "5", "8", "60"]
    */
  def fieldsToConvertToJsonArray: Map[String, Set[String]]

  /**
    * The names of JSON Arrays that should be converted from arrays Strings to arrays Doubles.
    * e.g: "24,81,5,8,60" to [24, 81, 5, 8, 60]
    */
  def fieldsToConvertFromJsonArrayStringToDouble: Set[String]

  /**
    * The names of JSON fields that should be renamed.
    * e.g: "kobe": "Bryant" to "Kobe": "Bryant"
    */
  def fieldsToRename: Map[String, String]

  /**
    * The names of JSON fields that should be removed.
    */
  def fieldsToRemove: Set[String]

  /**
    * The names of JSON fields that should be extracted to there own JSON.
    */
  def variantFieldsToExtract: Set[String]
}

case object FrequencyAnalysis extends V2FConstants {
  override def tableName = "Frequency Analysis"

  override def filePath: String = "frequency-analysis"

  override def fieldsToConvertToJsonDouble: Set[String] = Set(
    "eaf",
    "maf"
  )

  override def fieldsToConvertToJsonLong: Set[String] = Set(
    "position"
  )

  override def fieldsToConvertToJsonBoolean: Set[String] = Set.empty

  override def fieldsToConvertToJsonArray: Map[String, Set[String]] = Map.empty

  override def fieldsToConvertFromJsonArrayStringToDouble: Set[String] = Set.empty

  override def fieldsToRename: Map[String, String] = Map("var_id" -> "variant_id")

  override def fieldsToRemove: Set[String] =
    Set("chromosome", "position", "reference", "alt")

  override def variantFieldsToExtract: Set[String] =
    Set("chromosome", "position", "reference", "alt")
}

case object MetaAnalysisAncestrySpecific extends V2FConstants {
  override def tableName = "Ancestry-Specific Meta-Analysis"

  override def filePath: String = "meta-analysis/ancestry-specific"

  override def fieldsToConvertToJsonDouble: Set[String] = Set(
    "p_value",
    "beta",
    "std_err"
  )

  override def fieldsToConvertToJsonLong: Set[String] = Set(
    "n",
    "position"
  )

  override def fieldsToConvertToJsonBoolean: Set[String] = Set.empty

  override def fieldsToConvertToJsonArray: Map[String, Set[String]] = Map.empty

  override def fieldsToConvertFromJsonArrayStringToDouble: Set[String] = Set.empty

  override def fieldsToRename: Map[String, String] = Map("var_id" -> "variant_id")

  override def fieldsToRemove: Set[String] =
    Set("chromosome", "position", "reference", "alt")

  override def variantFieldsToExtract: Set[String] =
    Set("chromosome", "position", "reference", "alt")
}

case object MetaAnalysisTransEthnic extends V2FConstants {
  override def tableName = "Trans-Ethnic Meta-Analysis"

  override def filePath: String = "meta-analysis/trans-ethnic"

  override def fieldsToConvertToJsonDouble: Set[String] = Set(
    "p_value",
    "z_score",
    "std_err",
    "beta"
  )

  override def fieldsToConvertToJsonLong: Set[String] = Set(
    "n",
    "position"
  )

  override def fieldsToConvertToJsonBoolean: Set[String] = Set("top")

  override def fieldsToConvertToJsonArray: Map[String, Set[String]] = Map.empty

  override def fieldsToConvertFromJsonArrayStringToDouble: Set[String] = Set.empty

  override def fieldsToRename: Map[String, String] = Map("var_id" -> "variant_id")

  override def fieldsToRemove: Set[String] =
    Set("chromosome", "position", "reference", "alt")

  override def variantFieldsToExtract: Set[String] =
    Set("chromosome", "position", "reference", "alt")
}

case object VariantEffectRegulatoryFeatureConsequences extends V2FConstants {
  override def tableName = "Regulatory Feature Consequences"

  override def filePath: String = "variant-effect/regulatory-feature-consequences"

  override def fieldsToConvertToJsonDouble: Set[String] = Set.empty

  override def fieldsToConvertToJsonLong: Set[String] = Set.empty

  override def fieldsToConvertToJsonBoolean: Set[String] = Set(
    "pick"
  )

  override def fieldsToConvertToJsonArray: Map[String, Set[String]] = Map(
    "," -> Set("consequence_terms")
  )

  override def fieldsToConvertFromJsonArrayStringToDouble: Set[String] = Set.empty

  override def fieldsToRename: Map[String, String] = Map("id" -> "variant_id")

  override def fieldsToRemove: Set[String] = Set.empty

  override def variantFieldsToExtract: Set[String] = Set.empty
}

case object VariantEffectTranscriptConsequences extends V2FConstants {
  override def tableName = "Transcript Consequences"

  override def filePath: String = "variant-effect/transcript-consequences"

  override def fieldsToConvertToJsonDouble: Set[String] = Set(
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

  override def fieldsToConvertToJsonLong: Set[String] = Set(
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

  override def fieldsToConvertToJsonBoolean: Set[String] = Set(
    "canonical",
    "pick"
  )

  override def fieldsToConvertToJsonArray: Map[String, Set[String]] = Map(
    "," -> Set(
      "consequence_terms",
      "fathmm_pred",
      "fathmm_score",
      "flags",
      "lof_flags",
      "mutationtaster_aae",
      "mutationtaster_model",
      "mutationtaster_pred",
      "mutationtaster_score",
      "provean_pred",
      "provean_score",
      "sift_pred",
      "sift_score",
      "transcript_id_vest_3",
      "transcript_var_vest_3",
      "vest_3_score",
      "polyphen_2_hdiv_score",
      "polyphen_2_hvar_score",
      "interpro_domain"
    ),
    ":" -> Set(
      "siphy_29_way_pi"
    )
  )

  override def fieldsToConvertFromJsonArrayStringToDouble: Set[String] = Set(
    "mutationtaster_score",
    "siphy_29_way_pi",
    "vest_3_score",
    "polyphen_2_hdiv_score",
    "polyphen_2_hvar_score",
    "sift_score",
    "fathmm_score",
    "provean_score"
  )

  override def fieldsToRename: Map[String, String] = Map(
    "gerp++_nr" -> "gerp_plus_plus_nr",
    "gerp++_rs" -> "gerp_plus_plus_rs",
    "gerp++_rs_rankscore" -> "gerp_plus_plus_rs_rankscore",
    "id" -> "variant_id"
  )

  override def fieldsToRemove: Set[String] = Set.empty

  override def variantFieldsToExtract: Set[String] = Set.empty
}
