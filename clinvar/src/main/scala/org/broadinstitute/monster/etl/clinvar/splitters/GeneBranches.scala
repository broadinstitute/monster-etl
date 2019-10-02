package org.broadinstitute.monster.etl.clinvar.splitters

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.{SCollection, SideOutput}
import upack.{Msg, Obj, Str}

import scala.collection.mutable

/**
  * Pair of data streams produced by splitting variation-related
  * mapping info out of mapped gene entities.
  */
case class GeneBranches(
  genes: SCollection[Msg],
  geneAssociations: SCollection[Msg]
)

object GeneBranches {
  import org.broadinstitute.monster.etl.clinvar.ClinvarConstants._

  /**
    * Split a stream of mapped gene entities into a deduplicated
    * stream of genes and a stream of gene/variation associations.
    *
    * Gene deduplication is performed using the mapped gene ID (symbol).
    */
  def fromGeneStream(
    genes: SCollection[Msg]
  )(implicit coder: Coder[Msg]): GeneBranches = {
    val associations = SideOutput[Msg]

    val (main, side) = genes.withSideOutputs(associations).withName("Split Genes").map {
      (gene, ctx) =>
        val geneCopy = upack.copy(gene)
        val association = new mutable.LinkedHashMap[Msg, Msg]()

        val geneId = geneCopy.obj(IdKey)
        val associationFields = List(VarRef, Str("relationship_type"), Str("source"))

        association.update(GeneRef, geneId)
        associationFields.foreach { field =>
          geneCopy.obj.remove(field).foreach(association.update(field, _))
        }

        ctx.output(associations, Obj(association): Msg)
        geneCopy
    }

    GeneBranches(
      genes = main.distinctBy(_.obj(IdKey).str),
      geneAssociations = side(associations)
    )
  }
}
