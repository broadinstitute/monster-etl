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
  def tsvPattern: String

  /**
    * The names of JSON fields that should be converted from Strings to Doubles.
    */
  def fieldsToConvertToJsonDouble: List[String]

  /**
    * The names of JSON fields that should be converted from Strings to Integers.
    * Converting to int instead due to JSON schema int columns not accepting doubles
    */
  def fieldsToConvertToJsonInt: List[String]

  /**
    * The names of JSON fields that should be converted from Strings to a Booleans.
    */
  def fieldsToConvertToJsonBoolean: List[String]

  /**
    * The names of JSON fields that should be converted from from Strings to Arrays.
    * The keys of the map are delimiter and the values of map are names of JSON fields for the give delimiter(key).
    * e.g: "24,81,5,8,60" to ["24", "81", "5", "8", "60"]
    */
  def fieldsToConvertToJsonArray: Map[String, List[String]]

  /**
    * The names of JSON Arrays that should be converted from arrays Strings to arrays Doubles.
    * e.g: "24,81,5,8,60" to [24, 81, 5, 8, 60]
    */
  def fieldsToConvertFromJsonArrayStringToDouble: List[String]
}

case object FrequencyAnalysis extends V2FConstants {
  override def tableName = "Frequency Analysis"

  override def tsvPattern: String = "frequency-analysis"

  override def fieldsToConvertToJsonDouble: List[String] = List(
    "eaf",
    "maf"
  )

  override def fieldsToConvertToJsonInt: List[String] = Nil

  override def fieldsToConvertToJsonBoolean: List[String] = Nil

  override def fieldsToConvertToJsonArray: Map[String, List[String]] = Map(Nil -> Nil)

  override def fieldsToConvertFromJsonArrayStringToDouble: List[String] = Nil
}

case object MetaAnalysisAncestrySpecific extends V2FConstants {
  override def tableName = "Ancestry-Specific Meta-Analysis"

  override def tsvPattern: String = "meta-analysis/ancestry-specific"

  override def fieldsToConvertToJsonDouble: List[String] = List(
    "position",
    "p_value",
    "beta",
    "std_err"
  )

  override def fieldsToConvertToJsonInt: List[String] = List(
    "n"
  )

  override def fieldsToConvertToJsonBoolean: List[String] = Nil

  override def fieldsToConvertToJsonArray: Map[String, List[String]] = Map(Nil -> Nil)

  override def fieldsToConvertFromJsonArrayStringToDouble: List[String] = Nil
}

case object MetaAnalysisTransEthnic extends V2FConstants {
  override def tableName = "Trans-Ethnic Meta-Analysis"

  override def tsvPattern: String = "meta-analysis/trans-ethnic"

  override def fieldsToConvertToJsonDouble: List[String] = List(
    "position",
    "p_value",
    "z_score",
    "std_err"
  )

  override def fieldsToConvertToJsonInt: List[String] = List(
    "n"
  )

  override def fieldsToConvertToJsonBoolean: List[String] = Nil

  override def fieldsToConvertToJsonArray: Map[String, List[String]] = Map(Nil -> Nil)

  override def fieldsToConvertFromJsonArrayStringToDouble: List[String] = Nil
}

case object VariantEffectRegulatoryFeatureConsequences extends V2FConstants {
  override def tableName = "Regulatory Feature Consequences"

  override def tsvPattern: String = "variant-effect/regulatory-feature-consequences"

  override def fieldsToConvertToJsonDouble: List[String] = Nil

  override def fieldsToConvertToJsonInt: List[String] = Nil

  override def fieldsToConvertToJsonBoolean: List[String] = List(
    "pick"
  )

  override def fieldsToConvertToJsonArray: Map[String, List[String]] = Map(
    "," -> List("consequence_terms")
  )

  override def fieldsToConvertFromJsonArrayStringToDouble: List[String] = Nil
}

case object VariantEffectTranscriptConsequences extends V2FConstants {
  override def tableName = "Transcript Consequences"

  override def tsvPattern: String = "variant-effect/transcript-consequences"

  override def fieldsToConvertToJsonDouble: List[String] = List(
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
    "gerp++_nr",
    "gerp++_rs",
    "gerp++_rs_rankscore",
    "gm12878_confidence_value",
    "gm12878_fitcons_score",
    "gm12878_fitcons_score_rankscore",
    "h1_hesc_confidence_value",
    "h1_hesc_fitcons_score",
    "h1_hesc_fitcons_score_rankscore",
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
    "phastcons100way_vertebrate",
    "phastcons100way_vertebrate_rankscore",
    "phastcons20way_mammalian",
    "phastcons20way_mammalian_rankscore",
    "phylop100way_vertebrate",
    "phylop100way_vertebrate_rankscore",
    "phylop20way_mammalian",
    "phylop20way_mammalian_rankscore",
    "polyphen2_hdiv_rankscore",
    "polyphen2_hvar_rankscore",
    "polyphen_score",
    "provean_converted_rankscore",
    "sift_converted_rankscore",
    "siphy_29way_logodds",
    "siphy_29way_logodds_rankscore",
    "vest3_rankscore"
  )

  override def fieldsToConvertToJsonInt: List[String] = List(
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

  override def fieldsToConvertToJsonBoolean: List[(String)] = List(
    "canonical",
    "pick"
  )

  override def fieldsToConvertToJsonArray: Map[String, List[String]] = Map(
    "," -> List(
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
      "transcript_id_vest3",
      "transcript_var_vest3",
      "vest3_score",
      "polyphen2_hdiv_score",
      "polyphen2_hvar_score",
      "interpro_domain"
    ),
    ":" -> List(
      "siphy_29way_pi"
    )
  )

  override def fieldsToConvertFromJsonArrayStringToDouble: List[String] = List(
    "mutationtaster_score",
    "siphy_29way_pi",
    "vest3_score",
    "polyphen2_hdiv_score",
    "polyphen2_hvar_score"
  )
}
