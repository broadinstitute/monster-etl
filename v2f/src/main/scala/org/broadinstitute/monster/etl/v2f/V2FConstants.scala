package org.broadinstitute.monster.etl.v2f

/**
  * constants for v2f that allow for transforms of the converted tsv to json
  */
sealed trait V2FConstants {

  /**
    * the name of the tsv table that is being converted to Json and transformed
    */
  def tableName: String

  /**
    * relative path to the tsv that will be converted to Json and transformed
    */
  def tsvPath: String

  /**
    * the names of json fields that should be converted from a json string to a json double
    */
  def fieldsToConvertToJsonDouble: List[String]

  /**
    * the names of json fields that should be converted from a json string to a json double
    */
  def fieldsToConvertToJsonInt: List[String]

  /**
    * the names of json fields that should be converted from a json string to a json boolean
    */
  def fieldsToConvertToJsonBoolean: List[String]

  /**
    * the names json fields that should be converted from from a json string to a json array
    * the keys of the map are delimeter's and the values of map are names of json fields for the give delimeter(key)
    */
  def fieldsToConvertToJsonArray: Map[String, List[String]]

  /**
    * the json fields of a json array that should be converted from a json string to a json double
    */
  def fieldsToConvertFromJsonArrayStringToDouble: List[String]
}

case object FrequencyAnalysis extends V2FConstants {
  override def tableName = "Frequency Analysis"

  override def tsvPath: String = "frequencyanalysis/*/*"

  override def fieldsToConvertToJsonDouble: List[String] = List(
    "eaf",
    "maf"
  )

  override def fieldsToConvertToJsonInt: List[String] = ???

  override def fieldsToConvertToJsonBoolean: List[String] = ???

  override def fieldsToConvertToJsonArray: Map[String, List[String]] = ???

  override def fieldsToConvertFromJsonArrayStringToDouble: List[String] = ???
}

case object MetaAnalysisAncestrySpecific extends V2FConstants {
  override def tableName = "Meta Analysis Ancestry Specific"

  override def tsvPath: String = "metaanalysis/ancestry-specific/*/*/*"

  override def fieldsToConvertToJsonDouble: List[String] = List(
    "position",
    "p_value",
    "beta",
    "std_err"
  )

  override def fieldsToConvertToJsonInt: List[String] = List(
    "n"
  )

  override def fieldsToConvertToJsonBoolean: List[String] = ???

  override def fieldsToConvertToJsonArray: Map[String, List[String]] = ???

  override def fieldsToConvertFromJsonArrayStringToDouble: List[String] = ???
}

case object MetaAnalysisTransEthnic extends V2FConstants {
  override def tableName = "Meta Analysis Trans Ethnic"

  override def tsvPath: String = "metaanalysis/trans-ethnic/*/*"

  override def fieldsToConvertToJsonDouble: List[String] = List(
    "position",
    "p_value",
    "z_score",
    "std_err"
  )

  override def fieldsToConvertToJsonInt: List[String] = List(
    "n"
  )

  override def fieldsToConvertToJsonBoolean: List[String] = ???

  override def fieldsToConvertToJsonArray: Map[String, List[String]] = ???

  override def fieldsToConvertFromJsonArrayStringToDouble: List[String] = ???
}

case object VariantEffectRegulatoryFeatureConsequences extends V2FConstants {
  override def tableName = "Variant Effect Regulatory Feature Consequences"

  override def tsvPath: String = "varianteffect/regulatory_feature_consequences/*"

  override def fieldsToConvertToJsonDouble: List[String] = ???

  override def fieldsToConvertToJsonInt: List[String] = ???

  override def fieldsToConvertToJsonBoolean: List[String] = List(
    "pick"
  )

  override def fieldsToConvertToJsonArray: Map[String, List[String]] = Map(
    "," -> List("consequence_terms")
  )

  override def fieldsToConvertFromJsonArrayStringToDouble: List[String] = ???
}

case object VariantEffectTranscriptConsequences extends V2FConstants {
  override def tableName = "Variant Effect Transcript Consequences"

  override def tsvPath: String = "varianteffect/transcript_consequences/*"

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
