package org.broadinstitute.monster.etl.clinvar

import caseapp.{AppName, AppVersion, HelpMessage, ProgName}
import cats.data.NonEmptyList
import com.spotify.scio.ContextAndArgs
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.{SCollection, SideOutput}
import org.broadinstitute.monster.ClinvarBuildInfo
import org.broadinstitute.monster.etl.{MsgIO, UpackMsgCoder}
import upack._

import scala.collection.mutable

object ClinvarPipeline {

  implicit val msgCoder: Coder[Msg] = Coder.beam(new UpackMsgCoder)

  @AppName("ClinVar transformation pipeline")
  @AppVersion(ClinvarBuildInfo.version)
  @ProgName("org.broadinstitute.monster.etl.clinvar.ClinVarPipeline")
  case class Args(
    @HelpMessage("Path to the top-level directory where ClinVar XML was extracted")
    inputPrefix: String,
    @HelpMessage("Path where transformed ClinVar JSON should be written")
    outputPrefix: String
  )

  def main(rawArgs: Array[String]): Unit = {
    val (pipelineContext, parsedArgs) = ContextAndArgs.typed[Args](rawArgs)

    val fullArchives = MsgIO
      .readJsonLists(
        pipelineContext,
        "VariationArchive",
        s"${parsedArgs.inputPrefix}/VariationArchive/*.json"
      )

    val (vcvs, List(scvs, rcvs, variations)) = split(
      // VCV fields
      Set(
        NonEmptyList.of("@Accession"),
        NonEmptyList.of("@Version"),
        NonEmptyList.of("@DateCreated"),
        NonEmptyList.of("@DateLastUpdated"),
        NonEmptyList.of("@NumberOfSubmissions"),
        NonEmptyList.of("@NumberOfSubmitters"),
        NonEmptyList.of("RecordStatus"),
        NonEmptyList.of("InterpretedRecord", "ReviewStatus"),
        NonEmptyList.of("Species"),
        NonEmptyList.of("@ReleaseDate")
      ),
      // SCV fields (need further splitting)
      Set(
        NonEmptyList
          .of("InterpretedRecord", "ClinicalAssertionList", "ClinicalAssertion"),
        NonEmptyList.of("IncludedRecord", "ClinicalAssertionList", "ClinicalAssertion")
      ),
      // RCV fields
      Set(
        NonEmptyList.of("InterpretedRecord", "RCVList", "RCVAccession"),
        NonEmptyList.of(
          "InterpretedRecord",
          "ClinicalAssertionList",
          "ClinicalAssertion",
          "ObservedInList",
          "ObservedIn",
          "ObservedData"
        ),
        NonEmptyList.of("IncludedRecord", "RCVList", "RCVAccession"),
        NonEmptyList.of(
          "IncludedRecord",
          "ClinicalAssertionList",
          "ClinicalAssertion",
          "ObservedInList",
          "ObservedIn",
          "ObservedData"
        )
      ),
      // Variation fields
      Set(
        NonEmptyList.of("InterpretedRecord", "SimpleAllele"),
        NonEmptyList.of("InterpretedRecord", "Haplotype"),
        NonEmptyList.of("InterpretedRecord", "Genotype"),
        NonEmptyList.of("IncludedRecord", "SimpleAllele"),
        NonEmptyList.of("IncludedRecord", "Haplotype"),
        NonEmptyList.of("IncludedRecord", "Genotype")
      )
    )(fullArchives)

    val (assertions, List(submitters, submissions, assertionVariations)) = split(
      // Clinical Assertion fields
      Set(
        NonEmptyList.of("@ID"),
        NonEmptyList.of("ClinVarAccession", "@Accession"),
        NonEmptyList.of("ClinVarAccession", "@Version"),
        NonEmptyList.of("ClinVarAccession", "@title"),
        NonEmptyList.of("ClinVarAccession", "@localKey"),
        NonEmptyList.of("ClinVarAccession", "@Type"),
        NonEmptyList.of("@DateCreated"),
        NonEmptyList.of("@DateLastUpdated"),
        NonEmptyList.of("ClinVarSubmissionID", "@submittedAssembly"),
        NonEmptyList.of("RecordStatus"),
        NonEmptyList.of("ReviewStatus"),
        NonEmptyList.of("Interpretation", "Description"),
        NonEmptyList.of("Interpretation", "@DateLastEvaluated"),
        NonEmptyList.of("Interpretation", "Comment", "$"),
        NonEmptyList.of("Interpretation", "Comment", "@Type")
      ),
      // Submitter fields
      Set(
        NonEmptyList.of("ClinVarAccession", "@OrgID"),
        NonEmptyList.of("ClinVarAccession", "@SubmitterName"),
        NonEmptyList.of("ClinVarAccession", "@OrganizationCategory"),
        NonEmptyList.of("ClinVarAccession", "@OrgAbbreviation")
      ),
      // Submission fields
      Set(
        NonEmptyList.of("ClinVarAccession", "@OrgID"),
        NonEmptyList.of("@SubmissionDate"),
        NonEmptyList.of("ClinVarAccession", "@SubmitterName")
      ),
      // Clinical Assertion Variation fields
      Set(
        NonEmptyList.of("SimpleAllele", "@ID"),
        NonEmptyList.of("SimpleAllele", "VariantType"),
        NonEmptyList.of("Haplotype", "@ID"),
        NonEmptyList.of("Haplotype", "VariantType"),
        NonEmptyList.of("Genotype", "@ID"),
        NonEmptyList.of("Genotype", "VariantType")
      )
    )(scvs)

    MsgIO.writeJsonLists(
      vcvs,
      "VCV",
      s"${parsedArgs.outputPrefix}/variation_archive"
    )
    MsgIO.writeJsonLists(
      assertions,
      "SCV Clinical Assertion",
      s"${parsedArgs.outputPrefix}/clinical_assertion"
    )
    MsgIO.writeJsonLists(
      submitters,
      "SCV Submitter",
      s"${parsedArgs.outputPrefix}/submitter"
    )
    MsgIO.writeJsonLists(
      submissions,
      "SCV Submission",
      s"${parsedArgs.outputPrefix}/submission"
    )
    MsgIO.writeJsonLists(
      assertionVariations,
      "SCV Clinical Assertion Variation",
      s"${parsedArgs.outputPrefix}/clinical_assertion_variation"
    )
    MsgIO.writeJsonLists(
      rcvs,
      "RCV",
      s"${parsedArgs.outputPrefix}/rcv_accession"
    )
    MsgIO.writeJsonLists(
      variations,
      "Variation",
      s"${parsedArgs.outputPrefix}/variation_archive_variation"
    )

    pipelineContext.close()
    ()
  }

  /**
    * "Drill" into a upack message by descending through a chain of field names,
    * returning the value(s) found at the end of the chain.
    *
    * Arrays found along the chain will be kept as arrays, and further drilling
    * will be applied to each element in the collection.
    */
  def drill(fields: List[String])(msg: Msg): Option[Msg] =
    fields match {
      case Nil => Some(msg)
      case f :: fs =>
        msg match {
          case Obj(props) =>
            props.get(Str(f)).flatMap(drill(fs))
          case Arr(ms) =>
            val expanded = ms.map(drill(fs)).collect {
              // TODO: What if `v` is an array? Should we flatten?
              case Some(v) => v
            }
            Some(Arr(expanded))
          case _ => None
        }
    }

  /**
    * Split the fields of objects in a stream into multiple output streams.
    *
    * @param mainFields fields to keep in the "main" stream of outputs, which
    *                   produces one output per message in the input
    * @param splitFields sets of fields to split into separate streams. Split streams
    *                    can produce multiple outputs per message in the input
    */
  def split(
    mainFields: Set[NonEmptyList[String]],
    splitFields: Set[NonEmptyList[String]]*
  ): SCollection[Msg] => (SCollection[Msg], List[SCollection[Msg]]) = { inStream =>
    val sideOuts = List.fill(splitFields.length)(SideOutput[Msg])
    val zipped = splitFields.zip(sideOuts)

    val (mainOuts, side) = inStream.withSideOutputs(sideOuts: _*).map { (msg, ctx) =>
      val mainOut = new mutable.LinkedHashMap[Msg, Msg]()

      msg.obj.foreach {
        case (Str(k), v) =>
          mainFields.find(_.head == k).foreach { fields =>
            val newName = (fields.head :: fields.tail).mkString(".")
            drill(fields.tail)(v).foreach(mainOut.update(Str(newName), _))
          }
        case _ => ()
      }

      zipped.foreach {
        case (splitFields, sideOut) =>
          msg.obj.foreach {
            case (Str(k), v) =>
              splitFields.find(_.head == k).foreach { fields =>
                // FIXME: This is broken. We need a way to collect many "paths"
                // through the data in a single output.
                drill(fields.tail)(v).foreach {
                  case Arr(vs) => vs.foreach(ctx.output(sideOut, _))
                  case v       => ctx.output(sideOut, v)
                }
              }
            case _ => ()
          }
      }

      Obj(mainOut): Msg
    }

    (mainOuts, sideOuts.map(side(_)))
  }

  def split(
    rhsFields: Set[String],
    idLink: (String, String)
  ): SCollection[Msg] => (SCollection[Msg], SCollection[Msg]) = { inStream =>
    val sideOut = SideOutput[Msg]

    val (rhs, sideOuts) = inStream.withSideOutputs(sideOut).flatMap { (msg, ctx) =>
      val (lhsKey, rhsKey) = idLink
      val lhsId = msg.obj(Str(lhsKey))

      val lhsOut = new mutable.LinkedHashMap[Msg, Msg]()
      val rhsOut = new mutable.ArrayBuffer[Msg]()

      msg.obj.foreach {
        case (Str(k), v) if rhsFields.contains(k) =>
          v match {
            case Arr(vs) =>
              vs.foreach { inner =>
                val base = upack.copy(inner)
                base.obj.update(Str(rhsKey), lhsId)
                rhsOut.append(base)
              }
            case single =>
              val base = upack.copy(single)
              base.obj.update(Str(rhsKey), lhsId)
              rhsOut.append(base)
          }
        case (k, v) => lhsOut.update(k, v)
      }

      ctx.output(sideOut, Obj(lhsOut))
      rhsOut
    }

    (sideOuts(sideOut), rhs)
  }
}
