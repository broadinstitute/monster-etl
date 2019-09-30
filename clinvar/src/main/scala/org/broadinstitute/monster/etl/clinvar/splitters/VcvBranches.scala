package org.broadinstitute.monster.etl.clinvar.splitters

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.{SCollection, SideOutput}
import upack.{Msg, Obj, Str}

import scala.collection.mutable

/** TODO COMMENT */
case class VcvBranches(
  vcvs: SCollection[Msg],
  vcvReleases: SCollection[Msg]
)

object VcvBranches {
  import org.broadinstitute.monster.etl.clinvar.ClinvarContants._

  /** TODO COMMENT */
  def fromVcvStream(vcvs: SCollection[Msg])(implicit coder: Coder[Msg]): VcvBranches = {
    val releases = SideOutput[Msg]

    val (main, side) =
      vcvs.withSideOutputs(releases).withName("Split VCVs and releases").map {
        (vcv, ctx) =>
          val vcvCopy = upack.copy(vcv)
          val release = new mutable.LinkedHashMap[Msg, Msg]()

          val vcvId = vcvCopy.obj(IdKey)
          val vcvVersion = vcvCopy.obj(Str("version"))
          val vcvRelease = vcvCopy.obj(Str("release_date"))

          vcvCopy.obj.remove(Str("release_date"))

          release.update(VcvRef, vcvId)
          release.update(Str("version"), vcvVersion)
          release.update(Str("release_date"), vcvRelease)

          ctx.output(releases, Obj(release): Msg)
          vcvCopy
      }

    VcvBranches(
      vcvs = main,
      vcvReleases = side(releases)
    )
  }
}
