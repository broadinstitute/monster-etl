package org.broadinstitute.monster.etl

import com.spotify.scio.coders.Coder
import upack.Msg
import caseapp.core.help.Help
import caseapp.core.parser.Parser
import com.spotify.scio.{ContextAndArgs, ScioContext}

/**
  * Base class for all of our message-transforming pipelines.
  *
  * Provides common harness code for setting up / launching a scio pipeline.
  */
abstract class TransformationPipeline[Args: Parser: Help] {
  protected implicit val messageCoder: Coder[Msg] = Coder.beam(new UpackMsgCoder)

  final def main(rawArgs: Array[String]): Unit = {
    val (pipelineContext, parsedArgs) = ContextAndArgs.typed[Args](rawArgs)
    buildPipeline(pipelineContext, parsedArgs)

    pipelineContext.close()
    ()
  }

  /**
    * Register steps-to-run against the given pipeline context,
    * using args parsed from the command line.
    *
    * Methods like map, flatMap, etc. mutate the pipeline context
    * internally in ways that don't match the normal semantics of
    * those methods, so be careful with the build logic!
    */
  def buildPipeline(context: ScioContext, args: Args): Unit
}
