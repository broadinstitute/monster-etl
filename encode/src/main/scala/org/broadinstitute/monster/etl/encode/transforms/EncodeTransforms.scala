package org.broadinstitute.monster.etl.encode.transforms

import java.util.UUID

import io.circe.Json
import io.circe.syntax._
import org.broadinstitute.monster.etl.encode._

/**
  * Stream transformations run on all entity types during ENCODE ETL.
  */
object EncodeTransforms {

  /** Main ETL flow for all ENCODE entities which map to an entity type in our data model. */
  def cleanEntities(entityType: EncodeEntity): JsonPipe =
    _.transform(s"Common Prep - ${entityType.entryName}") {
      List(
        trimFields(entityType.fieldsToKeep),
        renameFields(entityType.fieldsToRename),
        buildLinks(entityType.linkFields),
        extractLabels(entityType.labelFields),
        collectAliases(entityType.aliasFields),
        assignIds
      ).reduce(_ andThen _)
    }

  // Cache the regex used below.
  private val labelRegex = "/[^/]+/(.+)/".r

  /**
    * Convert field values of the form '/<entity-type>/<the-label>/' to just '<the-label>'
    * in JSON objects passing through the stream.
    */
  private def extractLabels(labelFields: Set[String]): JsonPipe =
    _.transform("Extract Labels") {
      _.map { json =>
        labelFields.foldLeft(json) { (extractedSoFar, labelField) =>
          val extracted = for {
            idJson <- extractedSoFar(labelField)
            idString <- idJson.asString
            label <- labelRegex.findFirstMatchIn(idString)
          } yield {
            label
          }

          extracted
            .fold(extractedSoFar.remove(labelField)) { labelMatch =>
              extractedSoFar.add(labelField, labelMatch.group(1).asJson)
            }
        }
      }
    }

  /** Drop all fields but those in the given set from JSON objects passing through the stream. */
  private def trimFields(fieldsToKeep: Set[String]): JsonPipe =
    _.transform("Trim Fields")(_.map(_.filterKeys(fieldsToKeep)))

  /** Rename fields in the given map in JSON objects passing through the stream. */
  private def renameFields(fieldsToRename: List[(String, String)]): JsonPipe =
    _.transform("Rename Fields") {
      _.map { json =>
        fieldsToRename.foldLeft(json) {
          case (renamedSoFar, (oldName, newName)) =>
            json(oldName).fold(renamedSoFar) { value =>
              renamedSoFar.add(newName, value).remove(oldName)
            }
        }
      }
    }

  /**
    * Convert fields in the given set to HTTP links back to the ENCODE site in
    * objects passing through the stream.
    */
  private def buildLinks(linkFields: Set[String]): JsonPipe =
    _.transform("Build Links") {
      _.map { json =>
        linkFields.foldLeft(json) { (linkedSoFar, fieldName) =>
          val linkValue = for {
            valueJson <- linkedSoFar(fieldName)
            valueString <- valueJson.asString
          } yield {
            if (valueString.charAt(0) == '/') {
              s"https://www.encodeproject.org$valueString"
            } else {
              s"https://www.encodeproject.org/$valueString"
            }
          }

          linkValue.fold(linkedSoFar.remove(fieldName)) { link =>
            linkedSoFar.add(fieldName, link.asJson)
          }
        }
      }
    }

  /**
    * Flatten all the fields in the given set into a single "aliases" array
    * in JSON objects passing through the stream.
    */
  private def collectAliases(aliasFields: Set[String]): JsonPipe =
    _.transform(s"Collect Aliases") {
      _.map { json =>
        val allAliases = aliasFields.flatMap { field =>
          json(field).flatMap(_.asArray).fold(Set.empty[Json])(_.toSet)
        }
        json.filterKeys(!aliasFields.contains(_)).add("aliases", allAliases.asJson)
      }
    }

  /**
    * Assign unique Broad IDs to all JSON objects passing through the stream.
    *
    * Assigns random IDs for now. We need a real strategy for doing this in the future.
    */
  private def assignIds: JsonPipe =
    _.transform("Assign IDs") {
      _.map { json =>
        json.add("id", UUID.randomUUID().toString.asJson)
      }
    }
}
