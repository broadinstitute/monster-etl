package org.broadinstitute.monster.etl.clinvar.splitters

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.{SCollection, SideOutput}
import upack.{Msg, Obj, Str}

import scala.collection.mutable

/**
  * Collection of data streams produced by splitting submitter
  * and submission information out of a stream of mapped SCVs.
  */
case class ScvBranches(
  scvs: SCollection[Msg],
  submitters: SCollection[Msg],
  submissions: SCollection[Msg]
)

object ScvBranches {
  import org.broadinstitute.monster.etl.clinvar.ClinvarConstants._

  /**
    * Split a stream of mapped SCV entities into cross-linked streams
    * of submitters, submissions, and SCVs.
    *
    * The generated submitter and submission streams are deduplicated by ID.
    */
  def fromScvStream(scvs: SCollection[Msg])(implicit coder: Coder[Msg]): ScvBranches = {
    val submitterOut = SideOutput[Msg]
    val submissionOut = SideOutput[Msg]

    val (main, side) =
      scvs.withSideOutputs(submitterOut, submissionOut).withName("Split SCVs").map {
        (scv, ctx) =>
          val scvObj = scv.obj
          val newScv = new mutable.LinkedHashMap[Msg, Msg]()

          val scvId = scvObj(IdKey)
          val orgId = scvObj(Str("org_id"))
          val submitDate = scvObj(Str("submission_date"))
          val submissionId = Str(s"${orgId.str}.${submitDate.str}")

          val submitter = new mutable.LinkedHashMap[Msg, Msg]()
          val submission = new mutable.LinkedHashMap[Msg, Msg]()

          // Set and link IDs
          submitter.update(IdKey, orgId)
          submission.update(IdKey, submissionId)
          submission.update(SubmitterRef, orgId)
          newScv.update(IdKey, scvId)
          newScv.update(SubmitterRef, orgId)
          newScv.update(SubmissionRef, submissionId)

          // Fill in other properties
          val submitterFields =
            Set("submitter_name", "org_category", "org_abbrev").map(Str)
          val submissionFields =
            Set("submission_date", "submission_names").map(Str)
          (scvObj.keySet -- submitterFields -- submissionFields - Str("org_id")).foreach {
            k =>
              scvObj.get(k).foreach(newScv.update(k, _))
          }
          submitterFields.foreach { k =>
            scvObj.get(k).foreach(submitter.update(k, _))
          }
          submissionFields.foreach { k =>
            scvObj.get(k).foreach(submission.update(k, _))
          }

          // Push outputs.
          ctx.output(submitterOut, Obj(submitter): Msg)
          ctx.output(submissionOut, Obj(submission): Msg)
          Obj(newScv): Msg
      }

    ScvBranches(
      scvs = main,
      submitters = side(submitterOut).distinctBy(_.obj(IdKey).str),
      submissions = side(submissionOut).distinctBy(_.obj(IdKey).str)
    )
  }
}
