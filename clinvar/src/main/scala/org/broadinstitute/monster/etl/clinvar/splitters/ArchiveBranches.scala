package org.broadinstitute.monster.etl.clinvar.splitters

import java.util.concurrent.atomic.AtomicInteger

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.{SCollection, SideOutput}
import upack.{Arr, Msg, Obj, Str}

import scala.collection.mutable

/**
  * Collection of data streams produced by the initial splitting
  * operation performed on raw VariationArchive entries.
  */
case class ArchiveBranches(
  variations: SCollection[Msg],
  genes: SCollection[Msg],
  vcvs: SCollection[Msg],
  rcvs: SCollection[Msg],
  scvs: SCollection[Msg],
  scvVariations: SCollection[Msg],
  vaTraitSets: SCollection[Msg],
  vaTraits: SCollection[Msg]
  scvVariations: SCollection[Msg],
  scvObservations: SCollection[Msg],
  scvTraitSets: SCollection[Msg],
  scvTraits: SCollection[Msg]
)

object ArchiveBranches {
  import org.broadinstitute.monster.etl.clinvar.ClinvarConstants._

  /**
    * Split a stream of raw VariationArchive entries into multiple
    * streams of un-nested entities.
    *
    * Cross-linking between entities in the output streams occurs
    * prior to elements being pushed out of the split step.
    */
  def fromArchiveStream(
    archiveStream: SCollection[Msg]
  )(implicit coder: Coder[Msg]): ArchiveBranches = {

    val geneOut = SideOutput[Msg]
    val vcvOut = SideOutput[Msg]
    val rcvOut = SideOutput[Msg]
    val scvOut = SideOutput[Msg]
    val scvVariationOut = SideOutput[Msg]
    val vaTraitSets = SideOutput[Msg]
    val vaTraits = SideOutput[Msg]
    val scvObservationOut = SideOutput[Msg]
    val scvTraitSetOut = SideOutput[Msg]
    val scvTraitOut = SideOutput[Msg]

    val (variationStream, sideCtx) = archiveStream
      .withSideOutputs(
        geneOut,
        vcvOut,
        rcvOut,
        scvOut,
        scvVariationOut,
        vaTraitSets,
        vaTraits
        scvVariationOut,
        scvObservationOut,
        scvTraitSetOut,
        scvTraitOut
      )
      .withName("Split Variation Archives")
      .map { (fullVcv, ctx) =>
        val vcvObj = fullVcv.obj
        val recordCopy =
          upack.copy(vcvObj.getOrElse(InterpretedRecord, vcvObj(IncludedRecord)))

        // Pull out the variation for the archive.
        val topLevelVariation = getTopLevelVariation(recordCopy)
        // Link the variant to its children.
        // NOTE: The linking process pops child records out of their enclosing parents'
        // field sets. We assume that every curated variant will have a top-level
        // archive entry in the input, so we don't push the children as we pop them.
        val (vcvVariationId, _, _) =
          collectVariantIds(topLevelVariation, None)(_.obj(Str("@VariationID")))

        // Extract any genes associated with the variant.
        extractList(topLevelVariation, "GeneList", "Gene").foreach { gene =>
          gene.obj.update(VarRef, vcvVariationId)
          ctx.output(geneOut, gene)
        }

        // We only care about entities associated with "Interpreted" records,
        // which are backed by actual submissions.
        // The "Included" alternative is generated internally by ClinVar for DB consistency.
        if (vcvObj.contains(InterpretedRecord)) {
          val trimmedVcv = new mutable.LinkedHashMap[Msg, Msg]
          val vcvId = vcvObj(Str("@Accession"))

          // Link the VCV to its variation.
          trimmedVcv.update(VarRef, vcvVariationId)

          // Extract and push out any RCVs in the VCV.
          extractList(recordCopy, "RCVList", "RCVAccession").foreach { rcv =>
            val rcvObj = rcv.obj

            // Link back to VCV and variation.
            rcvObj.update(VcvRef, vcvId)
            rcvObj.update(VarRef, vcvVariationId)

            // Push to side output.
            ctx.output(rcvOut, rcv)
          }

          // Extract any SCVs in the record.
          // Extract and push out any SCVs and SCV-level variations.
          extractList(recordCopy, "ClinicalAssertionList", "ClinicalAssertion").foreach {
            scv =>
              val scvObj = scv.obj
              val scvId = scvObj(Str("ClinVarAccession")).obj(Str("@Accession"))

              // Link the SCV back to the VCV, top-level variant, and RCVs.
              scvObj.update(VcvRef, vcvId)
              scvObj.update(VarRef, vcvVariationId)

              // Extract out SCV-level variation.
              val scvVariation = getTopLevelVariation(scv)

              // SCV variations don't have a pre-set ID, so we have to manufacture one.
              val counter = new AtomicInteger(0)
              // Link SCV variations, pushing *each* variant.
              // There's no meaningful way to dedup variants across SCVs, so we just
              // capture all of them and live with the verbosity.
              val _ =
                collectVariantIds(scvVariation, Some { msg =>
                  val _ = ctx.output(scvVariationOut, msg)
                }) { scvVar =>
                  val id = Str(s"${scvId.str}.${counter.getAndIncrement()}")
                  scvVar.obj.update(IdKey, id)
                  // Link the variant to its SCV while we're at it.
                  scvVariation.obj.update(ScvRef, scvId)
                  id
                }

              /*
               * Common logic for extracting, linking, and pushing TraitSet nodes
               * out of SCV and SCV Observations.
               *
               * Traits nested within the TraitSet are also extracted, linked, and
               * pushed as part of this process.
               */
              def extractScvTraitSet(wrapper: Msg, id: Msg, ref: Msg): Unit =
                wrapper.obj.remove(Str("TraitSet")).foreach { traitSet =>
                  // Link the trait set back to its parent.
                  traitSet.obj.update(IdKey, id)
                  traitSet.obj.update(ref, id)

                  // Process any traits nested within the set.
                  val traitCounter = new AtomicInteger(0)
                  traitSet.obj
                    .remove(Str("Trait"))
                    .toIterable
                    .flatMap {
                      case Arr(traits) => traits
                      case other       => Iterable(other)
                    }
                    .foreach { `trait` =>
                      val traitId = Str(s"${id.str}.${traitCounter.getAndIncrement()}")
                      `trait`.obj.update(IdKey, traitId)
                      `trait`.obj.update(ScvTraitSetRef, id)
                      ctx.output(scvTraitOut, `trait`)
                    }

                  ctx.output(scvTraitSetOut, traitSet)
                }

              // Link and push SCV observations.
              counter.set(0)
              extractList(scv, "ObservedInList", "ObservedIn").foreach { observation =>
                val id = Str(s"${scvId.str}.${counter.getAndIncrement()}")
                observation.obj.update(IdKey, id)
                observation.obj.update(ScvRef, scvId)

                // Extract, link, and push the observed trait set (if any).
                extractScvTraitSet(observation, id, ScvObsRef)

                // Push out the observation.
                ctx.output(scvObservationOut, observation)
              }

              // Extract, link, and push the top-level trait set (if any).
              extractScvTraitSet(scv, scvId, ScvRef)

              // Push out the SCV.
              ctx.output(scvOut, scv)
          }

          // extract Variation Archive Trait Sets.
          extractList(recordCopy, "Interpretations", "Interpretation").foreach {
            interpretation =>
              val traitSets =
                // TODO do we need to anticipate the possibility of no ConditionList or no TraitSet?
                interpretation.obj(Str("ConditionList")).obj(Str("TraitSet")) match {
                  // the TraitSet tag might have one or multiple elements
                  case Arr(msgs) => msgs
                  case msg       => Iterable(msg)
                }
              traitSets.foreach { traitSet =>
                // add an entry for each traitSet element
                val traitSetObj = traitSet.obj
                traitSetObj.update(IdKey, traitSet.obj(Str("@ID")))
                traitSetObj.update(Str("type"), traitSet.obj(Str("@Type")))

                ctx.output(vaTraitSets, traitSet)

                // extract Variation Archive Traits.
                val traits = traitSet.obj(Str("Trait")) match {
                  // the Trait might have one or multiple elements
                  case Arr(msgs) => msgs
                  case msg       => Iterable(msg)
                }

                traits.foreach { oneTrait =>
                  val traitObj = oneTrait.obj
                  // from the "easy" way

                  // TraitID: This will always be pulled from the ConditionList element?
                  traitObj.update(Str("trait_id"), oneTrait.obj(Str("@ID")))

                  // Type: This will always be pulled from the ConditionList element?
                  traitObj.update(Str("type"), oneTrait.obj(Str("@Type")))

                  // Name: Might be in ConditionList or TraitMappingList?
                  // parse Name elements to find the one that is "preferred"
                  val names = oneTrait.obj(Str("Name")) match {
                    // Name might have one or multiple elements
                    case Arr(msgs) => msgs
                    case msg       => Iterable(msg)
                  }
                  names.foreach { name =>
                    // if the ElementValue node has a @Type of "Preferred" then use the "$" for the name
                    if (name
                          .obj(Str("ElementValue"))
                          .obj(Str("@Type"))
                          .str == "Preferred") {
                      traitObj.update(
                        Str("name"),
                        name.obj(Str("ElementValue")).obj(Str("$"))
                      )
                    }
                  }

                  // ID: This primary key might be in ConditionList or TraitMappingList
                  // parse XRef elements to find the one that is "MedGen" if it is present
                  // note that this is the primary key id for VariationArchiveTraits
                  // if XRef tag with MedGen ID doesn't exist, we look to the TraitMappingList

                  val maybeXref = oneTrait.obj.get(Str("XRef"))

                  val xrefs = maybeXref.fold {
                    Iterable.empty[Msg]
                  } {
                    case Arr(msgs) => msgs
                    case msg       => Iterable(msg)
                  }

                  xrefs.foreach { xref =>
                    // if the XRef element has a @DB of "MedGen" then use the @ID for the ID
                    if (xref.obj(Str("@DB")).str == "MedGen") {
                      traitObj.update(IdKey, xref.obj(Str("@ID")))
                    }
                  }

                  // TraitMappingList approach to get the ID
                  if (xrefs.isEmpty) {
                    extractList(recordCopy, "TraitMappingList", "TraitMapping").foreach {
                      allMappings =>
                        val traitMappings = allMappings match {
                          case Arr(msgs) => msgs
                          case msg       => Iterable(msg)
                        }
                        traitMappings.foreach { traitMapping =>
                          // 1. filter down by traitObj's "trait_id" == traitMapping's "@ClinicalAssertionID"
                          if (traitObj(Str("trait_id")) == traitMapping.obj(
                                Str("@ClinicalAssertionID")
                              ) &&
                              // 2. filter down by traitObj's "type" == traitMapping's "@TraitType"
                              traitObj(Str("type")) == traitMapping.obj(
                                Str("@TraitType")
                              )) {
                            traitMapping.obj(Str("@MappingType")).str match {
                              // 3a. if traitMapping's "@MappingType" is "Name", then filter by
                              //      traitObj's "name" == traitMapping's "@MappingValue"
                              //      and pull out traitMapping's "Medgen", "@CUI" value
                              case "Name" =>
                                if (traitObj(Str("name")) == traitMapping.obj(
                                      Str("@MappingValue")
                                    )) {
                                  traitObj.update(
                                    IdKey,
                                    traitMapping.obj(Str("MedGen")).obj(Str("@CUI"))
                                  )
                                }
                              // 3b. if traitMapping's "@MappingType" is "XRef", then look through oneTrait's
                              //     XRef elements and match on oneTrait's "@DB" and "@ID" to traitMapping's
                              //      "@MappingRef" and "@MappingValue" respectively; if matched, then use
                              //      traitMapping's "MedGen", "@CUI" value
                              case "XRef" =>
                                xrefs.foreach { xref =>
                                  if (xref.obj(Str("@DB")) == traitMapping.obj(
                                        Str("@MappingRef")
                                      ) &&
                                      xref.obj(Str("@ID")) == traitMapping.obj(
                                        Str("@CUI")
                                      )) {
                                    traitObj.update(
                                      IdKey,
                                      traitMapping.obj(Str("MedGen")).obj(Str("@CUI"))
                                    )
                                  }
                                }
                            }
                          }
                        }
                    }
                  }
                  ctx.output(vaTraits, oneTrait)
                }
              }
          }

          // Re-inject whatever fields are left in the record, along
          // with any top-level fields for the VCV.
          vcvObj.foreach {
            case (InterpretedRecord, _) =>
              trimmedVcv.update(InterpretedRecord, recordCopy)
            case (k, v) => trimmedVcv.update(k, v)
          }
          ctx.output(vcvOut, Obj(trimmedVcv): Msg)
        }

        // Always output the top-level variation.
        topLevelVariation
      }

    ArchiveBranches(
      variations = variationStream,
      genes = sideCtx(geneOut),
      vcvs = sideCtx(vcvOut),
      rcvs = sideCtx(rcvOut),
      scvs = sideCtx(scvOut),
      scvVariations = sideCtx(scvVariationOut),
      vaTraitSets = sideCtx(vaTraitSets),
      vaTraits = sideCtx(vaTraits)
      scvVariations = sideCtx(scvVariationOut),
      scvObservations = sideCtx(scvObservationOut),
      scvTraitSets = sideCtx(scvTraitSetOut),
      scvTraits = sideCtx(scvTraitOut)
    )
  }

  /**
    * Pop and return a list of entries from a nested field within a message.
    *
    * XML sections with repeated tags are typically structured like:
    *
    *   <WrapperTag>
    *     <RepeatedTag></RepeatedTag>
    *     <RepeatedTag></RepeatedTag>
    *   </WrapperTag>
    *
    * Our XML->JSON converter munges things a little bit, by:
    *   1. Always typing `WrapperTag` as an object containing a single field named `RepeatedTag`
    *   2. Typing `RepeatedTag` as an array if multiple instances of the tag were present, and
    *      otherwise typing it as a non-array
    *
    * This extraction method assumes this conversion convention, descending through some wrapper
    * layer to pull out the maybe-array. If the nested value is not an array, it is wrapped by
    * an `Iterable` before being returned.
    *
    * @param msg the entity containing the nested list to extract
    * @param wrapperName name of the tag / field which contains the potentially-repeated field
    * @param elementName name of the repeated tag / field which is nested within `wrapperName`
    * @return all values of `elementName` found under `wrapperName`
    */
  def extractList(msg: Msg, wrapperName: String, elementName: String): Iterable[Msg] = {
    val maybeList = for {
      wrapper <- msg.obj.remove(Str(wrapperName))
      arrayOrSingle <- wrapper.obj.get(Str(elementName))
    } yield {
      arrayOrSingle match {
        case Arr(msgs) => msgs
        case msg       => Iterable(msg)
      }
    }

    maybeList.getOrElse(Iterable.empty)
  }

  /**
    * Extract a single top-level variation record from a wrapper message.
    *
    * The extracted variation will be popped from the wrapper message's field map.
    * If no variant is found, this method will fail with an exception.
    */
  def getTopLevelVariation(msg: Msg): Msg =
    VariationTypes
      .foldLeft(Option.empty[Msg]) { (acc, subtype) =>
        acc.orElse(getVariation(msg, subtype))
      }
      .getOrElse {
        throw new RuntimeException(s"No variant found in entity: $msg")
      }

  /**
    * Extract a specific subclass of variation from a wrapper message, if present.
    *
    * If found, the extracted variation will be popped from the wrapper message's field map.
    * Extracted messages will be tagged with their original subclass type, to avoid losing data.
    */
  def getVariation(msg: Msg, subclassType: Msg): Option[Msg] =
    msg.obj.remove(subclassType).map {
      case arr @ Arr(many) =>
        many.foreach(_.obj.update(SubclassKey, subclassType))
        arr
      case other =>
        other.obj.update(SubclassKey, subclassType)
        other
    }

  /**
    * Collect the IDs of a variation, its children, and all of its descendants.
    *
    * The process of ID collection will pop child variation out of their parents'
    * field maps. To avoid losing data, this method exposes an optional hook which
    * can be used to push a variation to a side output after it has been cross-linked.
    *
    * @param variantMessage the variation entry to traverse for IDs
    * @param output optional hook which can be used to push un-nested variations
    *               to a side output after cross-linking
    * @param getId method which can pull an ID out of a variation method
    */
  def collectVariantIds(
    variantMessage: Msg,
    output: Option[Msg => Unit]
  )(getId: Msg => Msg): (Msg, List[Msg], List[Msg]) = {
    val immediateId = getId(variantMessage)
    val zero = (List.empty[Msg], List.empty[Msg])
    val (childIds, descendantIds) = VariationTypes.foldLeft(zero) {
      case ((childAcc, descendandsAcc), subtype) =>
        val (children, descendants) = getVariation(variantMessage, subtype).fold(zero) {
          case Arr(children) =>
            children.foldLeft(zero) {
              case ((childAcc, descandantsAcc), child) =>
                val (childId, grandChildIds, deepDescendants) =
                  collectVariantIds(child, output)(getId)
                (
                  childId :: childAcc,
                  grandChildIds ::: deepDescendants ::: descandantsAcc
                )
            }
          case child =>
            val (childId, grandChildIds, deepDescendants) =
              collectVariantIds(child, output)(getId)
            (List(childId), grandChildIds ::: deepDescendants)
        }
        (children ::: childAcc, descendants ::: descendandsAcc)
    }
    variantMessage.obj.update(ChildrenRef, Arr(childIds: _*))
    variantMessage.obj.update(DescendantsRef, Arr(childIds ::: descendantIds: _*))
    output.foreach(_.apply(variantMessage))
    (immediateId, childIds, descendantIds)
  }
}
