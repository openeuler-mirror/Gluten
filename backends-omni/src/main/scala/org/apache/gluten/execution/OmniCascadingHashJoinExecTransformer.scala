
package org.apache.gluten.execution

import com.google.protobuf.{Any, StringValue}
import io.substrait.proto.JoinRel
import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.extension.ValidationResult
import org.apache.gluten.metrics.MetricsUpdater
import org.apache.gluten.substrait.{JoinParams, SubstraitContext}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression}
import org.apache.spark.sql.catalyst.optimizer.{BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans.{Inner, InnerLike, JoinType}
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, BuildSideRelation, HashJoin, ShuffledHashJoinExec}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.collection.Seq
import scala.collection.mutable.ArrayBuffer

/**
 * One native CascadingJoin level.
 *
 * streamedProjects:
 *   ProjectExec nodes located on THIS join's streamed/left input before the
 *   next inner hash join.
 *
 * Example:
 *
 *   BHJ79
 *     Project76
 *       BHJ75
 *
 * then Project76 belongs to joinInfo(79), not joinInfo(75).
 *
 * This implementation accepts only transparent ProjectExec nodes whose
 * projectList consists entirely of Attribute references. They are column
 * pruning/reordering operations and can be folded without materializing
 * intermediate VectorBatch objects.
 */
case class CascadingJoinInfo(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    condition: Option[Expression],
    buildSide: BuildSide,
    joinType: JoinType,
    buildPlan: SparkPlan,
    isBHJ: Boolean,
    streamedProjects: Seq[ProjectExec] = Seq.empty)

/**
 * Native fused cascading hash join.
 *
 * IMPORTANT:
 *   `output` is the PHYSICAL expanded output produced by the native
 *   CascadingLookupJoin:
 *
 *       rootProbe.output ++ build0.output ++ build1.output ++ ...
 *
 * Intermediate transparent Projects are intentionally NOT materialized inside
 * the native cascading operator. OmniCascadingJoinRule adds one final
 * ProjectExec above this transformer when Spark's original top join output is
 * a pruned/reordered subset of this expanded physical output.
 */
case class OmniCascadingHashJoinExecTransformer(
    probePlan: SparkPlan,
    joinChain: Seq[CascadingJoinInfo],
    originalOutput: Seq[Attribute])
  extends TransformSupport {

  require(joinChain.nonEmpty, "Join chain cannot be empty")

  val cascadeLevels: Int = joinChain.length

  lazy val physicalOutput: Seq[Attribute] =
    probePlan.output ++ joinChain.flatMap(_.buildPlan.output)

  override def children: Seq[SparkPlan] =
    probePlan +: joinChain.map(_.buildPlan)

  override def output: Seq[Attribute] =
    physicalOutput

  override def nodeName: String =
    s"OmniCascadingHashJoin(cascadeLevels=$cascadeLevels)"

  @transient override lazy val metrics: Map[String, SQLMetric] =
    BackendsApiManager
      .getMetricsApiInstance
      .genHashJoinTransformerMetrics(sparkContext)

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager
      .getMetricsApiInstance
      .genHashJoinTransformerMetricsUpdater(metrics)

  private def normalizedKeys(
      joinInfo: CascadingJoinInfo): (Seq[Expression], Seq[Expression]) = {
    if (BackendsApiManager.getSettings.enableJoinKeysRewrite()) {
      (
        HashJoin.rewriteKeyExpr(joinInfo.leftKeys),
        HashJoin.rewriteKeyExpr(joinInfo.rightKeys))
    } else {
      (joinInfo.leftKeys, joinInfo.rightKeys)
    }
  }

  private def buildHashTableId(joinInfo: CascadingJoinInfo): String = {
    if (joinInfo.isBHJ) {
      "BuiltHashTable-" + joinInfo.buildPlan.id
    } else {
      ""
    }
  }

  override protected def doValidateInternal(): ValidationResult = {
    OmniCascadingHashJoinExecTransformer
      .validateChain(probePlan, joinChain, originalOutput) match {
      case Some(reason) =>
        ValidationResult.failed(reason)
      case None =>
        ValidationResult.succeeded
    }
  }

  /**
   * Build a continuous nested JoinRel chain.
   *
   * Intermediate transparent ProjectExec nodes are NOT emitted as ProjectRel.
   * Their only semantic effect is column pruning/reordering. Because every
   * accepted streamed/hash key must still resolve from rootProbe.output, the
   * native probe ordinal remains stable and there is no need to materialize
   * A+B / A+B+C intermediate rows.
   *
   * Spark's original output schema is restored by the final ProjectExec that
   * OmniCascadingJoinRule places above this transformer.
   */
  override protected def doTransform(
      context: SubstraitContext): TransformContext = {

    val probeCtx =
      probePlan
        .asInstanceOf[TransformSupport]
        .transform(context)

    var currentRelNode =
      probeCtx.root

    // Physical/native schema. Do NOT apply intermediate Project pruning here.
    var currentOutput =
      probeCtx.outputAttributes

    val operatorId =
      context.nextOperatorId(this.nodeName)

    val joinParams =
      new JoinParams

    joinParams.isCascading = true
    joinParams.cascadeLevels = cascadeLevels

    // Residual conditions are intentionally NOT sent to native CascadingJoin.
    //
    // OmniCascadingJoinRule hoists deterministic residual conditions to one
    // Spark FilterExec above the expanded CascadingJoin output. Native
    // CascadingLookupJoin currently has no per-level residual evaluator.

    joinChain.zipWithIndex.foreach {
      case (joinInfo, level) =>
        require(
          joinInfo.buildSide == BuildRight,
          s"Cascading level $level must be BuildRight")

        val (streamedKeyExprs, buildKeyExprs) =
          normalizedKeys(joinInfo)

        val buildCtx =
          joinInfo.buildPlan
            .asInstanceOf[TransformSupport]
            .transform(context)

        val buildRelNode =
          buildCtx.root

        val buildOutput =
          buildCtx.outputAttributes

        /*
         * IMPORTANT:
         *
         * Join expressions are resolved against `currentOutput`, which is the
         * expanded physical schema rather than the Project-pruned logical
         * schema. Attribute exprIds remain unchanged for transparent Projects,
         * so OmniJoinUtils can resolve the correct physical ordinal.
         *
         * Native therefore continues to receive:
         *
         *   JoinRel
         *     left = JoinRel
         *       left = JoinRel
         *
         * instead of:
         *
         *   JoinRel
         *     left = ProjectRel
         *       input = JoinRel
         *
         * This preserves one CascadingLookupJoinOperator with no intermediate
         * materialization.
         */
        val joinRel =
          OmniJoinUtils.createJoinRel(
            streamedKeyExprs,
            buildKeyExprs,
            None, // residual condition is evaluated by the Rule's final FilterExec
            JoinRel.JoinType.JOIN_TYPE_INNER,
            false,
            joinInfo.joinType,
            genCascadingJoinParameters(level, joinInfo),
            currentRelNode,
            buildRelNode,
            currentOutput,
            buildOutput,
            context,
            operatorId,
            null)

        currentRelNode =
          joinRel

        currentOutput =
          currentOutput ++ buildOutput
    }

    context.registerJoinParam(
      operatorId,
      joinParams)

    TransformContext(
      physicalOutput,
      currentRelNode)
  }

  private def genCascadingJoinParameters(
      level: Int,
      joinInfo: CascadingJoinInfo): Any = {

    val isBhjValue =
      if (joinInfo.isBHJ) 1 else 0

    val hashTableId =
      buildHashTableId(joinInfo)

    val joinParametersStr =
      new StringBuffer("JoinParameters:")
        .append("isCascading=1\n")
        .append("cascadeLevels=").append(cascadeLevels).append("\n")
        .append("currentLevel=").append(level).append("\n")
        .append("probeFromRoot=1\n")
        .append("isBHJ=").append(isBhjValue).append("\n")
        .append("isNullAwareAntiJoin=0\n")
        .append("buildHashTableId=").append(hashTableId).append("\n")
        .append("isBuildLeft=0\n")
        .append("isExistenceJoin=0\n")

    val message =
      StringValue
        .newBuilder()
        .setValue(joinParametersStr.toString)
        .build()

    BackendsApiManager
      .getTransformerApiInstance
      .packPBMessage(message)
  }

  override def columnarInputRDDs: Seq[RDD[ColumnarBatch]] = {
    val probeRDDs =
      getColumnarInputRDDs(probePlan)

    val buildRDDs =
      joinChain.zipWithIndex.flatMap {
        case (joinInfo, _) if joinInfo.isBHJ =>
          val broadcastRelation =
            joinInfo.buildPlan.executeBroadcast[BuildSideRelation]()

          Seq(
            OmniBroadcastBuildSideRDD(
              sparkContext,
              broadcastRelation))

        case (joinInfo, _) =>
          getColumnarInputRDDs(joinInfo.buildPlan)
      }

    probeRDDs ++ buildRDDs
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[SparkPlan]): SparkPlan = {

    require(
      newChildren.length == cascadeLevels + 1,
      s"Expected ${cascadeLevels + 1} children, actual=${newChildren.length}")

    val newProbePlan =
      newChildren.head

    val newBuildPlans =
      newChildren.tail

    val newJoinChain =
      joinChain
        .zip(newBuildPlans)
        .map {
          case (info, newBuild) =>
            info.copy(buildPlan = newBuild)
        }

    copy(
      probePlan = newProbePlan,
      joinChain = newJoinChain)
  }
}

object OmniCascadingHashJoinExecTransformer {

  private val minCascadeLevels = 2

  private def isInner(joinType: JoinType): Boolean =
    joinType == Inner ||
      joinType.isInstanceOf[InnerLike]

  private def supportedTopBhj(
      join: BroadcastHashJoinExec): Boolean = {
    isInner(join.joinType) &&
      join.buildSide == BuildRight &&
      !join.isNullAwareAntiJoin
  }

  private def supportedTopShj(
      join: ShuffledHashJoinExec): Boolean = {
    isInner(join.joinType) &&
      join.buildSide == BuildRight
  }

  private def supportedMiddleBhj(
      join: BroadcastHashJoinExec): Boolean =
    supportedTopBhj(join)

  /**
   * A transparent Project can be folded into the cascading physical schema.
   *
   * We intentionally restrict v1 to direct Attribute references:
   *
   *   Project(a, b, c)
   *   Project(c, a)
   *
   * Unsupported here:
   *
   *   Project(a + 1 AS x)
   *   Project(cast(a AS bigint))
   *   Project(concat(a, b))
   *
   * Computed Projects need a native per-level expression evaluator analogous
   * to HashBuilderWithExprOperator. Do not silently skip them.
   */
  private def isTransparentProject(project: ProjectExec): Boolean =
    project.projectList.forall(_.isInstanceOf[Attribute])

  /**
   * Peel zero or more transparent ProjectExec nodes from a join's streamed
   * input. Returned projects are ordered from nearest-to-current-join to
   * deepest.
   */
  private def peelTransparentProjects(
      plan: SparkPlan): (SparkPlan, Seq[ProjectExec]) = {

    val projects =
      ArrayBuffer.empty[ProjectExec]

    var current = plan

    var keepGoing = true

    while (keepGoing) {
      current match {
        case project: ProjectExec if isTransparentProject(project) =>
          projects += project
          current = project.child

        case _ =>
          keepGoing = false
      }
    }

    (current, projects.toSeq)
  }

  private def bhjInfo(
      join: BroadcastHashJoinExec,
      streamedProjects: Seq[ProjectExec] = Seq.empty): CascadingJoinInfo = {
    CascadingJoinInfo(
      leftKeys = join.leftKeys,
      rightKeys = join.rightKeys,
      condition = join.condition,
      buildSide = join.buildSide,
      joinType = join.joinType,
      buildPlan = join.right,
      isBHJ = true,
      streamedProjects = streamedProjects)
  }

  private def shjInfo(
      join: ShuffledHashJoinExec,
      streamedProjects: Seq[ProjectExec] = Seq.empty): CascadingJoinInfo = {
    CascadingJoinInfo(
      leftKeys = join.leftKeys,
      rightKeys = join.rightKeys,
      condition = join.condition,
      buildSide = join.buildSide,
      joinType = join.joinType,
      buildPlan = join.right,
      isBHJ = false,
      streamedProjects = streamedProjects)
  }

  /**
   * Extract the maximal supported cascading suffix.
   *
   * Supported example:
   *
   *   BHJ79 BuildRight
   *     Project76
   *       BHJ75 BuildRight
   *         BHJ68 BuildRight
   *           Project62
   *             BHJ61 BuildRight
   *               BHJ42 BuildLeft   <- boundary/rootProbe
   *
   * Result:
   *
   *   rootProbe = BHJ42
   *   chain = [61, 68, 75, 79]
   *
   *   info61.streamedProjects = []
   *   info68.streamedProjects = [62]
   *   info75.streamedProjects = []
   *   info79.streamedProjects = [76]
   */
  def extractJoinChain(
      plan: SparkPlan): Option[(SparkPlan, Seq[CascadingJoinInfo])] = {

    val candidate =
      plan match {

        case top: BroadcastHashJoinExec if supportedTopBhj(top) =>
          collectBhj(
            top,
            Seq.empty)

        case top: ShuffledHashJoinExec if supportedTopShj(top) =>
          val (nextPlan, projects) =
            peelTransparentProjects(top.left)

          nextPlan match {
            case lowerBhj: BroadcastHashJoinExec
                if supportedMiddleBhj(lowerBhj) =>
              val topInfo =
                shjInfo(top, projects)

              collectBhj(
                lowerBhj,
                Seq(topInfo))

            case _ =>
              val topInfo =
                shjInfo(top, Seq.empty)

              finishAtBoundary(
                top.left,
                Seq(topInfo))
          }

        case _ =>
          None
      }

    candidate.flatMap {
      case (rootProbe, chain) =>
        validateChain(rootProbe, chain, plan.output) match {
          case None => Some((rootProbe, chain))
          case Some(_) => None
        }
    }
  }

  /**
   * Collect one accepted BHJ and recursively walk its streamed side.
   *
   * The Project metadata is attached to the CURRENT/UPPER join.
   */
  private def collectBhj(
      join: BroadcastHashJoinExec,
      chainAbove: Seq[CascadingJoinInfo]):
      Option[(SparkPlan, Seq[CascadingJoinInfo])] = {

    val (nextPlan, projects) =
      peelTransparentProjects(join.left)

    nextPlan match {

      case lowerBhj: BroadcastHashJoinExec
          if supportedMiddleBhj(lowerBhj) =>
        val currentInfo =
          bhjInfo(
            join,
            projects)

        // inner -> outer order
        val newChain =
          currentInfo +: chainAbove

        collectBhj(
          lowerBhj,
          newChain)

      case _ =>
        val currentInfo =
          bhjInfo(
            join,
            Seq.empty)

        // inner -> outer order
        val newChain =
          currentInfo +: chainAbove

        finishAtBoundary(
          join.left,
          newChain)
    }
  }

  private def finishAtBoundary(
      rootProbe: SparkPlan,
      chain: Seq[CascadingJoinInfo]): Option[(SparkPlan, Seq[CascadingJoinInfo])] = {
    if (chain.length >= minCascadeLevels) {
      Some((rootProbe, chain))
    } else {
      None
    }
  }

  /**
   * Semantic validation for native root-probe Cascading execution.
   *
   * Native still uses probeFromRoot=1, so every accepted join key must be
   * evaluable from rootProbe.output. Transparent Projects may hide/reorder
   * columns logically, but they do not create a new value and therefore do not
   * invalidate root-probe semantics.
   */
  def validateChain(
      rootProbe: SparkPlan,
      chain: Seq[CascadingJoinInfo],
      originalOutput: Seq[Attribute]): Option[String] = {

    if (chain.length < minCascadeLevels) {
      return Some(
        s"Cascading join requires at least $minCascadeLevels levels, " +
          s"actual=${chain.length}")
    }

    if (!chain.forall(info => isInner(info.joinType))) {
      return Some(
        "Cascading join supports Inner/InnerLike only")
    }

    if (!chain.forall(_.buildSide == BuildRight)) {
      return Some(
        "Root-probe Cascading join supports BuildRight only")
    }

    // Only the outermost/top level may be SHJ.
    if (!chain.dropRight(1).forall(_.isBHJ)) {
      return Some(
        "Every middle cascading level must be BroadcastHashJoinExec")
    }

    // Defensive validation: every absorbed Project must remain transparent.
    chain.zipWithIndex.foreach {
      case (info, level) =>
        info.streamedProjects.foreach { project =>
          if (!isTransparentProject(project)) {
            return Some(
              s"Level $level contains a non-transparent ProjectExec")
          }

          val childExprIds =
            project.child.output.map(_.exprId).toSet

          if (!project.output.forall(a => childExprIds.contains(a.exprId))) {
            return Some(
              s"Level $level ProjectExec introduces a new expression/exprId")
          }
        }
    }

    val rootOutput =
      rootProbe.output

    val rootAttrs =
      AttributeSet(rootOutput)

    chain.zipWithIndex.foreach {
      case (info, level) =>

        val buildOutput =
          info.buildPlan.output

        val buildAttrs =
          AttributeSet(buildOutput)

        // Join key must be visible after the nearest streamed Project.
        val logicalStreamedAttrs =
          info.streamedProjects.headOption
            .map(p => AttributeSet(p.output))
            .getOrElse(rootAttrs)

        if (!referencesWithin(
              info.leftKeys,
              logicalStreamedAttrs)) {
          return Some(
            s"Level $level streamed key is not visible after its ProjectExec")
        }

        // Native probeFromRoot=1 safety condition.
        if (!referencesWithin(
              info.leftKeys,
              rootAttrs)) {
          return Some(
            s"Level $level streamed/hash key references attributes outside " +
              s"rootProbe output")
        }

        if (!referencesWithin(
              info.rightKeys,
              buildAttrs)) {
          return Some(
            s"Level $level build key references attributes outside current build")
        }

        info.condition.foreach { condition =>
          if (!condition.deterministic) {
            return Some(
              s"Level $level contains a non-deterministic residual condition")
          }
        }

        val allowedConditionAttrs =
          rootAttrs ++ buildAttrs

        if (!conditionWithin(
              info.condition,
              allowedConditionAttrs)) {
          return Some(
            s"Level $level residual condition references a previous build " +
              s"or unrelated attributes")
        }
    }

    /*
     * The native operator physically outputs the expanded schema:
     *
     *   root ++ build0 ++ build1 ++ ...
     *
     * The original Spark top join may expose only a subset/reordered set because
     * of intermediate Projects. That is valid as long as every original output
     * Attribute exists in the expanded physical schema. OmniCascadingJoinRule
     * adds a final ProjectExec to restore exact order/pruning.
     */
    val expandedOutput =
      rootOutput ++ chain.flatMap(_.buildPlan.output)

    val expandedExprIds =
      expandedOutput.map(_.exprId).toSet

    if (!originalOutput.forall(a => expandedExprIds.contains(a.exprId))) {
      return Some(
        "Original hash-join output contains an Attribute that is not available " +
          "from expanded Cascading output")
    }

    None
  }

  private def referencesWithin(
      expressions: Seq[Expression],
      allowed: AttributeSet): Boolean = {
    expressions.forall { expression =>
      expression.references.forall(allowed.contains)
    }
  }

  private def conditionWithin(
      condition: Option[Expression],
      allowed: AttributeSet): Boolean = {
    condition.forall { expression =>
      expression.references.forall(allowed.contains)
    }
  }

  def sameExprIds(
      left: Seq[Attribute],
      right: Seq[Attribute]): Boolean = {
    left.length == right.length &&
      left.zip(right).forall {
        case (l, r) =>
          l.exprId == r.exprId
      }
  }
}
