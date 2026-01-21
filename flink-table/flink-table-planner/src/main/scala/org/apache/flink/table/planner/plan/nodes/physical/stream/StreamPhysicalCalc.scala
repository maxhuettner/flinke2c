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
package org.apache.flink.table.planner.plan.nodes.physical.stream

import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, InputProperty}
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecCalc
import org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig
import org.apache.flink.table.api.TableException
import org.apache.flink.table.functions.{FunctionDefinition, ScalarFunction, ScalarFunctionDefinition}
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction
import org.apache.flink.table.planner.functions.utils.ScalarSqlFunction

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.Calc
import org.apache.calcite.rex.{RexCall, RexFieldAccess, RexInputRef, RexLiteral, RexNode, RexProgram, RexShuttle}
import org.apache.calcite.sql.SqlKind

import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

/** Stream physical RelNode for [[Calc]]. */
class StreamPhysicalCalc(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    calcProgram: RexProgram,
    outputRowType: RelDataType)
  extends StreamPhysicalCalcBase(cluster, traitSet, inputRel, calcProgram, outputRowType) {

  override def copy(traitSet: RelTraitSet, child: RelNode, program: RexProgram): Calc = {
    new StreamPhysicalCalc(cluster, traitSet, child, program, outputRowType)
  }

  override def translateToExecNode(): ExecNode[_] = {
    val projection = calcProgram.getProjectList.map(calcProgram.expandLocalRef)
    val condition = if (calcProgram.getCondition != null) {
      calcProgram.expandLocalRef(calcProgram.getCondition)
    } else {
      null
    }
    StreamPhysicalCalc.LOG.info(
      "StreamPhysicalCalc translateToExecNode: projectionSize={}, conditionPresent={}",
      projection.size,
      condition != null)

    val rewriter = new StreamPhysicalCalc.ProxyScalarFunctionRewriter
    val rewrittenProjection = projection.map(_.accept(rewriter))
    val rewrittenCondition = if (condition != null) {
      condition.accept(rewriter)
    } else {
      null
    }
    StreamPhysicalCalc.LOG.info(
      "StreamPhysicalCalc proxy rewrite: hasProxyFunction={}, proxyConfPresent={}",
      rewriter.hasProxyFunction,
      !rewriter.getProxyConf.isEmpty)

    if (rewriter.hasProxyFunction) {
      StreamPhysicalCalc.LOG.info(
        "Proxy rewrite matched scalar UDF in StreamPhysicalCalc, conf={}",
        rewriter.getProxyConf)
      new StreamExecCalc(
        unwrapTableConfig(this),
        rewrittenProjection,
        rewrittenCondition,
        InputProperty.DEFAULT,
        FlinkTypeFactory.toLogicalRowType(getRowType),
        getRelDetailedDescription,
        rewriter.getProxyConf)
    } else {
      new StreamExecCalc(
        unwrapTableConfig(this),
        projection,
        condition,
        InputProperty.DEFAULT,
        FlinkTypeFactory.toLogicalRowType(getRowType),
        getRelDetailedDescription)
    }
  }
}

private object StreamPhysicalCalc {
  private val LOG = LoggerFactory.getLogger(classOf[StreamPhysicalCalc])
  private val source = String.valueOf(classOf[StreamPhysicalCalc].getProtectionDomain.getCodeSource)
  LOG.warn("StreamPhysicalCalc object loaded from {}", source)
  System.err.println("StreamPhysicalCalc object loaded from " + source)
  private val CustomProxyFunctionName = "CurrencyConversionFunction"
  private val CustomProxyFunctionClassName = "org.example.flinke2c.CurrencyConversionFunction"

  private def matchesOperatorName(name: String): Boolean = {
    if (name == null) {
      return false
    }
    val trimmed = name.trim
    if (trimmed.isEmpty) {
      return false
    }
    val normalized = trimmed.replace("\"", "").replace("`", "")
    if (normalized.equalsIgnoreCase(CustomProxyFunctionName)) {
      return true
    }
    val lastDot = normalized.lastIndexOf('.')
    if (lastDot >= 0 && lastDot < normalized.length - 1) {
      val simple = normalized.substring(lastDot + 1)
      if (simple.equalsIgnoreCase(CustomProxyFunctionName)) {
        return true
      }
    }
    val suffixIndex = normalized.indexOf('$')
    if (suffixIndex > 0) {
      return normalized.substring(0, suffixIndex).equalsIgnoreCase(CustomProxyFunctionName)
    }
    false
  }

  private def matchesDefinition(definition: FunctionDefinition): Boolean = {
    if (definition == null) {
      return false
    }
    definition match {
      case scalarDef: ScalarFunctionDefinition =>
        CustomProxyFunctionClassName == scalarDef.getScalarFunction.getClass.getName
      case scalar: ScalarFunction =>
        CustomProxyFunctionClassName == scalar.getClass.getName
      case _ => false
    }
  }

  private def isProxyScalarFunction(call: RexCall): Boolean = {
    val operator = call.getOperator
    if (matchesOperatorName(operator.getName)) {
      return true
    }
    operator match {
      case scalar: ScalarSqlFunction =>
        CustomProxyFunctionClassName == scalar.scalarFunction.getClass.getName
      case bridging: BridgingSqlFunction =>
        val identifier = bridging.getResolvedFunction.getIdentifier
        val identifierName =
          if (identifier.isPresent) identifier.get.getFunctionName else null
        if (matchesOperatorName(identifierName)) {
          return true
        }
        matchesDefinition(bridging.getDefinition)
      case _ => false
    }
  }

  private def isSupportedProxyOperand(operand: RexNode): Boolean = {
    operand match {
      case _: RexInputRef => true
      case _: RexFieldAccess => true
      case call: RexCall =>
        if (call.getKind == SqlKind.CAST || call.getKind == SqlKind.AS) {
          isSupportedProxyOperand(call.getOperands.get(0))
        } else {
          false
        }
      case _ => false
    }
  }

  private def mergeProxyConf(existing: String, conf: String): String = {
    if (conf == null || conf.isEmpty) {
      return if (existing == null) "" else existing
    }
    if (existing == null || existing.isEmpty) {
      return conf
    }
    if (existing != conf) {
      throw new TableException(
        "Proxy scalar function requires a single, consistent conf literal.")
    }
    existing
  }

  private def extractProxyConf(operands: java.util.List[RexNode]): String = {
    val iterator = operands.iterator()
    while (iterator.hasNext) {
      val operand = iterator.next()
      operand match {
        case literal: RexLiteral =>
          val conf = RexLiteral.stringValue(literal)
          if (conf != null) {
            return conf
          }
        case _ =>
      }
      if (operand.getKind == SqlKind.DEFAULT) {
        return ""
      }
    }
    ""
  }

  private class ProxyScalarFunctionRewriter extends RexShuttle {
    private var proxyFunctionFound = false
    private var proxyConf: String = null

    override def visitCall(call: RexCall): RexNode = {
      if (!isProxyScalarFunction(call)) {
        return super.visitCall(call)
      }
      val operands = call.getOperands
      if (operands.isEmpty) {
        throw new TableException("Proxy scalar function requires at least one argument.")
      }
      val operand = operands.get(0)
      if (!isSupportedProxyOperand(operand)) {
        throw new TableException(
          "Proxy scalar function requires a column reference as its first argument.")
      }
      proxyFunctionFound = true
      proxyConf = mergeProxyConf(proxyConf, extractProxyConf(operands))
      operand.accept(this)
    }

    def hasProxyFunction: Boolean = proxyFunctionFound

    def getProxyConf: String = if (proxyConf == null) "" else proxyConf
  }
}
