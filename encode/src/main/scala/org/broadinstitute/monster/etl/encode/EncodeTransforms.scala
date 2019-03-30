package org.broadinstitute.monster.etl.encode

import com.spotify.scio.values.SCollection
import io.circe.{Json, JsonObject}
import io.circe.syntax._

object EncodeTransforms {
  type JsonPipe = SCollection[JsonObject] => SCollection[JsonObject]

  def cleanEntities(entityType: EncodeEntity): JsonPipe = { stream =>
    val name = entityType.entryName
    stream
      .transform(s"Trim $name Fields")(trimFields(entityType.fieldsToKeep))
      .transform(s"Rename $name Fields")(renameFields(entityType.fieldsToRename))
      .transform(s"Build $name Links")(buildLinks(entityType.linkFields))
      .transform(s"Extract $name Labels")(extractLabels)
      .transform(s"Combine $name Aliases")(collectAliases(entityType.aliasFields))
  }

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

          json.add("phenotype", splitPhenotypes.asJson)
      }
    }
  }

  def extractFileQc: JsonPipe = _.transform("Extract File QC") {
    _.map { json =>
      import io.circe.parser.parse

      val maybePercentAligned = for {
        // Drill down to QC wrapper:
        encodedNotes <- json("notes").flatMap(_.asString)
        notesObject <- parse(encodedNotes).toOption.flatMap(_.asObject)
        qcObject <- notesObject("qc")
          .flatMap(_.asObject)
          .flatMap(_("qc"))
          .flatMap(_.asObject)
        // Extract out QC values we care about:
        alignedReads <- qcObject("mapped")
          .flatMap(_.as[Array[Long]].toOption)
          .flatMap(_.headOption)
        totalReads <- qcObject("in_total")
          .flatMap(_.as[Array[Long]].toOption)
          .flatMap(_.headOption)
      } yield {
        alignedReads.toDouble / totalReads
      }

      maybePercentAligned
        .fold(json)(pct => json.add("percent_aligned_reads", pct.asJson))
        .remove("notes")
    }
  }

  private val extractLabels: JsonPipe = { stream =>
    val idRegex = "/[^/]+/(.+)/".r

    stream.map { json =>
      List("label", "lab").foldLeft(json) { (extractedSoFar, labelField) =>
        val extracted = for {
          idJson <- extractedSoFar(labelField)
          idString <- idJson.asString
          label <- idRegex.findFirstMatchIn(idString)
        } yield {
          label
        }

        extracted
          .fold(extractedSoFar.remove(labelField)) { labelMatch =>
            json.add(labelField, labelMatch.group(1).asJson)
          }
      }
    }
  }

  private def trimFields(fieldsToKeep: Set[String]): JsonPipe =
    _.map(_.filterKeys(fieldsToKeep))

  private def renameFields(fieldsToRename: Map[String, String]): JsonPipe =
    _.map { json =>
      fieldsToRename.foldLeft(json) {
        case (renamedSoFar, (oldName, newName)) =>
          renamedSoFar(oldName).fold(renamedSoFar) { value =>
            renamedSoFar.add(newName, value).remove(oldName)
          }
      }
    }

  private def buildLinks(linkFields: Set[String]): JsonPipe =
    _.map { json =>
      linkFields.foldLeft(json) { (linkedSoFar, fieldName) =>
        val linkValue = for {
          valueJson <- linkedSoFar(fieldName)
          valueString <- valueJson.asString
        } yield {
          if (valueString.charAt(0) == '/') {
            s"http://www.encodeproject.org$valueString"
          } else {
            s"http://www.encodeproject.org/$valueString"
          }
        }

        linkValue.fold(linkedSoFar.remove(fieldName)) { link =>
          linkedSoFar.add(fieldName, link.asJson)
        }
      }
    }

  private def collectAliases(aliasFields: Set[String]): JsonPipe =
    _.map { json =>
      val allAliases = aliasFields.flatMap { field =>
        json(field).flatMap(_.asArray).fold(Set.empty[Json])(_.toSet)
      }
      json.filterKeys(!aliasFields.contains(_)).add("aliases", allAliases.asJson)
    }
}
