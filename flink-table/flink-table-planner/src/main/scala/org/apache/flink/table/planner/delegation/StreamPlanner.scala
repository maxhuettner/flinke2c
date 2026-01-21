/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.table.planner.delegation

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.dag.Transformation
import org.apache.flink.configuration.ExecutionOptions
import org.apache.flink.configuration.PipelineOptions
import org.apache.flink.streaming.api.graph.StreamGraph
import org.apache.flink.streaming.api.operators.ChainingStrategy
import org.apache.flink.streaming.api.transformations.PhysicalTransformation
import org.apache.flink.table.api._
import org.apache.flink.table.catalog.{CatalogManager, FunctionCatalog}
import org.apache.flink.table.delegation.{Executor, InternalPlan}
import org.apache.flink.table.module.ModuleManager
import org.apache.flink.table.operations.Operation
import org.apache.flink.table.planner.plan.`trait`._
import org.apache.flink.table.planner.plan.ExecNodeGraphInternalPlan
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecCalc
import org.apache.flink.table.planner.plan.nodes.exec.processor.ExecNodeGraphProcessor
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodePlanDumper
import org.apache.flink.table.planner.plan.optimize.{Optimizer, StreamCommonSubGraphBasedOptimizer}
import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil
import org.apache.flink.table.planner.utils.DummyStreamExecutionEnvironment

import _root_.scala.collection.JavaConversions._
import org.apache.calcite.plan.{ConventionTraitDef, RelTrait, RelTraitDef}
import org.apache.calcite.sql.SqlExplainLevel
import org.slf4j.LoggerFactory

import java.util

import scala.collection.mutable

class StreamPlanner(
    executor: Executor,
    tableConfig: TableConfig,
    moduleManager: ModuleManager,
    functionCatalog: FunctionCatalog,
    catalogManager: CatalogManager,
    classLoader: ClassLoader)
  extends PlannerBase(
    executor,
    tableConfig,
    moduleManager,
    functionCatalog,
    catalogManager,
    isStreamingMode = true,
    classLoader) {
  private val LOG = LoggerFactory.getLogger(classOf[StreamPlanner])

  override protected def getTraitDefs: Array[RelTraitDef[_ <: RelTrait]] = {
    Array(
      ConventionTraitDef.INSTANCE,
      FlinkRelDistributionTraitDef.INSTANCE,
      MiniBatchIntervalTraitDef.INSTANCE,
      ModifyKindSetTraitDef.INSTANCE,
      UpdateKindTraitDef.INSTANCE,
      DuplicateChangesTraitDef.INSTANCE
    )
  }

  override protected def getOptimizer: Optimizer = new StreamCommonSubGraphBasedOptimizer(this)

  override def getExecNodeGraphProcessors: Seq[ExecNodeGraphProcessor] = Seq()

  override protected def translateToPlan(execGraph: ExecNodeGraph): util.List[Transformation[_]] = {
    beforeTranslation()
    val planner = createDummyPlanner()
    val transformations = execGraph.getRootNodes.map {
      case node: StreamExecNode[_] => node.translateToPlan(planner)
      case _ =>
        throw new TableException(
          "Cannot generate DataStream due to an invalid logical plan. " +
            "This is a bug and should not happen. Please file an issue.")
    }
    afterTranslation()
    val result = transformations ++ planner.extraTransformations
    applyProxyChainOnlyOverrides(result)
    result
  }

  private def applyProxyChainOnlyOverrides(
      transformations: util.List[Transformation[_]]): Unit = {
    if (!getTableConfig.get(CommonExecCalc.PROXY_CHAIN_ONLY_OPTION)) {
      return
    }
    if (!getTableConfig.get(PipelineOptions.OPERATOR_CHAINING)) {
      LOG.info(
        "Proxy chain-only enabled; forcing pipeline.operator-chaining.enabled to true.")
      getTableConfig.set(PipelineOptions.OPERATOR_CHAINING, Boolean.box(true))
    }
    val all = collectTransformations(transformations)
    if (!all.exists(t => isProxyPre(t) || isProxyPost(t))) {
      return
    }
    val upstreamOfPre = util.Collections.newSetFromMap(
      new util.IdentityHashMap[Transformation[_], java.lang.Boolean]())
    val downstreamOfPost = util.Collections.newSetFromMap(
      new util.IdentityHashMap[Transformation[_], java.lang.Boolean]())
    all.foreach { t =>
      if (isProxyPre(t)) {
        t.getInputs.foreach(upstreamOfPre.add)
      }
      if (hasProxyPostInput(t)) {
        downstreamOfPost.add(t)
      }
    }
    all.foreach {
      case t: PhysicalTransformation[_] =>
        if (isProxyPre(t)) {
          t.setChainingStrategy(ChainingStrategy.ALWAYS)
        } else if (isProxyPost(t)) {
          t.setChainingStrategy(ChainingStrategy.HEAD)
        } else if (upstreamOfPre.contains(t)) {
          t.setChainingStrategy(ChainingStrategy.ALWAYS)
        } else if (downstreamOfPost.contains(t)) {
          t.setChainingStrategy(ChainingStrategy.ALWAYS)
        } else {
          t.setChainingStrategy(ChainingStrategy.NEVER)
        }
      case _ => ()
    }
  }

  private def collectTransformations(
      roots: util.List[Transformation[_]]): util.Set[Transformation[_]] = {
    val visited = util.Collections.newSetFromMap(
      new util.IdentityHashMap[Transformation[_], java.lang.Boolean]())
    def visit(t: Transformation[_]): Unit = {
      if (visited.add(t)) {
        t.getInputs.foreach(visit)
      }
    }
    roots.foreach(visit)
    visited
  }

  private def hasProxyPostInput(transformation: Transformation[_]): Boolean = {
    transformation.getInputs.exists(isProxyPost)
  }

  private def isProxyPre(transformation: Transformation[_]): Boolean = {
    hasNameFragment(transformation, "ProxyPre")
  }

  private def isProxyPost(transformation: Transformation[_]): Boolean = {
    hasNameFragment(transformation, "ProxyPost")
  }

  private def hasNameFragment(
      transformation: Transformation[_],
      fragment: String): Boolean = {
    val name = transformation.getName
    name != null && name.contains(fragment)
  }

  override def explain(
      operations: util.List[Operation],
      format: ExplainFormat,
      extraDetails: ExplainDetail*): String = {
    if (format != ExplainFormat.TEXT) {
      throw new UnsupportedOperationException(
        s"Unsupported explain format [${format.getClass.getCanonicalName}]")
    }
    val (sinkRelNodes, optimizedRelNodes, execGraph, streamGraph) = getExplainGraphs(operations)

    val sb = new mutable.StringBuilder
    sb.append("== Abstract Syntax Tree ==")
    sb.append(System.lineSeparator)
    sinkRelNodes.foreach {
      sink =>
        // use EXPPLAN_ATTRIBUTES to make the ast result more readable
        // and to keep the previous behavior
        sb.append(FlinkRelOptUtil.toString(sink, SqlExplainLevel.EXPPLAN_ATTRIBUTES))
        sb.append(System.lineSeparator)
    }

    val withAdvice = extraDetails.contains(ExplainDetail.PLAN_ADVICE)
    if (withAdvice) {
      sb.append("== Optimized Physical Plan With Advice ==")
    } else {
      sb.append("== Optimized Physical Plan ==")
    }
    sb.append(System.lineSeparator)
    val explainLevel = if (extraDetails.contains(ExplainDetail.ESTIMATED_COST)) {
      SqlExplainLevel.ALL_ATTRIBUTES
    } else {
      SqlExplainLevel.DIGEST_ATTRIBUTES
    }
    val withChangelogTraits = extraDetails.contains(ExplainDetail.CHANGELOG_MODE)
    if (withAdvice) {
      sb.append(
        FlinkRelOptUtil
          .toString(
            optimizedRelNodes,
            explainLevel,
            withChangelogTraits = withChangelogTraits,
            withAdvice = true))
    } else {
      optimizedRelNodes.foreach {
        rel =>
          sb.append(
            FlinkRelOptUtil.toString(rel, explainLevel, withChangelogTraits = withChangelogTraits))
          sb.append(System.lineSeparator)
      }
    }

    sb.append("== Optimized Execution Plan ==")
    sb.append(System.lineSeparator)
    sb.append(ExecNodePlanDumper.dagToString(execGraph))

    if (extraDetails.contains(ExplainDetail.JSON_EXECUTION_PLAN)) {
      sb.append(System.lineSeparator)
      sb.append("== Physical Execution Plan ==")
      sb.append(System.lineSeparator)
      sb.append(streamGraph.getStreamingPlanAsJSON)
    }

    sb.toString()
  }

  private def createDummyPlanner(): StreamPlanner = {
    val dummyExecEnv = new DummyStreamExecutionEnvironment(getExecEnv)
    val executor = new DefaultExecutor(dummyExecEnv)
    new StreamPlanner(
      executor,
      tableConfig,
      moduleManager,
      functionCatalog,
      catalogManager,
      classLoader)
  }

  override def explainPlan(plan: InternalPlan, extraDetails: ExplainDetail*): String = {
    beforeTranslation()
    val execGraph = plan.asInstanceOf[ExecNodeGraphInternalPlan].getExecNodeGraph
    val transformations = translateToPlan(execGraph)
    afterTranslation()

    // We pass only the configuration to avoid reconfiguration with the rootConfiguration
    val streamGraph = executor
      .createPipeline(transformations, tableConfig.getConfiguration, null)
      .asInstanceOf[StreamGraph]

    val sb = new StringBuilder
    sb.append("== Optimized Execution Plan ==")
    sb.append(System.lineSeparator)
    sb.append(ExecNodePlanDumper.dagToString(execGraph))

    if (extraDetails.contains(ExplainDetail.JSON_EXECUTION_PLAN)) {
      sb.append(System.lineSeparator)
      sb.append("== Physical Execution Plan ==")
      sb.append(System.lineSeparator)
      sb.append(streamGraph.getStreamingPlanAsJSON)
    }

    sb.toString()
  }

  override def beforeTranslation(): Unit = {
    super.beforeTranslation()
    val runtimeMode = getTableConfig.get(ExecutionOptions.RUNTIME_MODE)
    if (runtimeMode != RuntimeExecutionMode.STREAMING) {
      throw new IllegalArgumentException(
        "Mismatch between configured runtime mode and actual runtime mode. " +
          "Currently, the 'execution.runtime-mode' can only be set when instantiating the " +
          "table environment. Subsequent changes are not supported. " +
          "Please instantiate a new TableEnvironment if necessary."
      )
    }
  }
}
