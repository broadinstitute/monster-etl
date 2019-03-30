package org.broadinstitute.monster.etl.encode

import enumeratum.{Enum, EnumEntry}

sealed trait EncodeEntity extends EnumEntry {

  def fieldsToKeep: Set[String] = Set(
    "@id",
    "accession",
    "aliases",
    "award",
    "date_created",
    "dbxrefs",
    "lab",
    "status",
    "submitted_by"
  )

  def fieldsToRename: Map[String, String] = Map(
    "@id" -> "label",
    "accession" -> "close_match",
    "award" -> "sponsor",
    "date_created" -> "created_at"
  )

  def linkFields: Set[String] = Set("close_match", "sponsor", "submitted_by")

  def aliasFields: Set[String] = Set("aliases", "dbxrefs")
}

object EncodeEntity extends Enum[EncodeEntity] {

  override val values = findValues

  case object Donor extends EncodeEntity {
    override def fieldsToKeep: Set[String] = super.fieldsToKeep.union(
      Set(
        "age_units",
        "age",
        "ethnicity",
        "external_ids",
        "health_status",
        "life_stage",
        "organism",
        "sex"
      )
    )

    override def fieldsToRename: Map[String, String] = super.fieldsToRename ++ Map(
      "health_status" -> "phenotype",
      "organism" -> "organism_id"
    )

    override def aliasFields: Set[String] = super.aliasFields + "external_ids"
  }

  case object Experiment extends EncodeEntity {
    override def fieldsToKeep: Set[String] = super.fieldsToKeep.union(
      Set(
        "date_released",
        "date_submitted",
        "description",
        "target"
      )
    )

    override def linkFields: Set[String] = super.linkFields + "target"
  }

  case object File extends EncodeEntity {
    override def fieldsToKeep: Set[String] = super.fieldsToKeep.union(
      Set(
        "file_format",
        "file_format_type",
        "file_size",
        "mapped_read_length",
        "mapped_run_type",
        "md5sum",
        "notes",
        "output_type",
        "paired_end",
        "platform",
        "read_count",
        "read_length",
        "run_type",
        "s3_uri",
        "status"
      )
    )

    override def fieldsToRename: Map[String, String] = super.fieldsToRename ++ Map(
      "file_format" -> "format",
      "file_format_type" -> "file_sub_type",
      "paired_end" -> "paired_end_identifier",
      "platform" -> "platform_id",
      "run_type" -> "paired_end",
      "s3_uri" -> "file_path"
    )

    override def aliasFields: Set[String] = super.aliasFields + "external_ids"
  }

  case object Library extends EncodeEntity {
    override def fieldsToKeep: Set[String] = super.fieldsToKeep + "strand_specificity"

    override def fieldsToRename: Map[String, String] =
      super.fieldsToRename + ("strand_specificity" -> "strand_specific")
  }

  case object Sample extends EncodeEntity {
    override def fieldsToKeep: Set[String] = super.fieldsToKeep.union(
      Set(
        "biosample_term_id",
        "biosample_type",
        "date_obtained",
        "source"
      )
    )

    override def fieldsToRename: Map[String, String] = super.fieldsToRename ++ Map(
      "biosample_term_id" -> "organ_id",
      "biosample_type" -> "biosample_type_id"
    )
  }
}
