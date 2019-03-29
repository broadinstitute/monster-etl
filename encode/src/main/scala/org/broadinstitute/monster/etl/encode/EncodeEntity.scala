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
