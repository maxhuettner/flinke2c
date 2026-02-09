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
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecCalc
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
    if (StreamPhysicalCalc.LOG.isDebugEnabled) {
      StreamPhysicalCalc.LOG.debug(
        "StreamPhysicalCalc translateToExecNode: projectionSize={}, conditionPresent={}",
        projection.size,
        condition != null)
    }

    val tableConfig = unwrapTableConfig(this)
    val proxyFunctionClass =
      tableConfig.getOptional(CommonExecCalc.PROXY_FUNCTION_CLASS_OPTION)
        .orElse(CommonExecCalc.CUSTOM_PROXY_FUNCTION_CLASS_NAME)

    val rewriter =
      new StreamPhysicalCalc.ProxyScalarFunctionRewriter(
        inputRel.getRowType,
        getRowType,
        proxyFunctionClass)
    val rewrittenProjection = projection.zipWithIndex.map {
      case (node, idx) =>
        rewriter.setCurrentOutputFieldIndex(idx)
        node.accept(rewriter)
    }
    rewriter.setCurrentOutputFieldIndex(-1)
    val rewrittenCondition = if (condition != null) {
      condition.accept(rewriter)
    } else {
      null
    }
    val effectiveCondition =
      if (rewriter.hasProxyFunction &&
        CommonExecCalc.PROXY_FUNCTION_KIND_FILTER == rewriter.getProxyFunctionKind) {
        null
      } else {
        rewrittenCondition
      }
    if (StreamPhysicalCalc.LOG.isDebugEnabled) {
      StreamPhysicalCalc.LOG.debug(
        s"StreamPhysicalCalc proxy rewrite: hasProxyFunction=${rewriter.hasProxyFunction}, " +
          s"functionKind=${rewriter.getProxyFunctionKind}")
    }

    if (rewriter.hasProxyFunction) {
      if (StreamPhysicalCalc.LOG.isDebugEnabled) {
        StreamPhysicalCalc.LOG.debug(
          "Proxy rewrite matched function: class={}, kind={}",
          rewriter.getProxyFunctionClass,
          rewriter.getProxyFunctionKind)
      }
      new StreamExecCalc(
        tableConfig,
        rewrittenProjection,
        effectiveCondition,
        InputProperty.DEFAULT,
        FlinkTypeFactory.toLogicalRowType(getRowType),
        getRelDetailedDescription,
        rewriter.getProxyConf,
        rewriter.getProxyFieldIndex.orNull,
        rewriter.getProxyFieldName,
        rewriter.getProxyFunctionClass,
        rewriter.getProxyFunctionKind,
        rewriter.getProxyArgFieldIndices,
        rewriter.getProxyArgFieldNames,
        rewriter.getProxyArgFieldTypes,
        rewriter.getProxyResultFieldIndices,
        rewriter.getProxyResultFieldNames,
        rewriter.getProxyResultFieldTypes,
        rewriter.getProxyResultUdfFieldTypes,
        rewriter.getProxyResultUdfFieldIndices)
    } else {
      new StreamExecCalc(
        tableConfig,
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

  private def matchesOperatorName(name: String, targetSimpleName: String): Boolean = {
    if (name == null) {
      return false
    }
    val trimmed = name.trim
    if (trimmed.isEmpty) {
      return false
    }
    val normalized = trimmed.replace("\"", "").replace("`", "")
    if (targetSimpleName != null && normalized.equalsIgnoreCase(targetSimpleName)) {
      return true
    }
    val lastDot = normalized.lastIndexOf('.')
    if (lastDot >= 0 && lastDot < normalized.length - 1) {
      val simple = normalized.substring(lastDot + 1)
      if (targetSimpleName != null && simple.equalsIgnoreCase(targetSimpleName)) {
        return true
      }
    }
    val suffixIndex = normalized.indexOf('$')
    if (suffixIndex > 0) {
      return targetSimpleName != null &&
        normalized.substring(0, suffixIndex).equalsIgnoreCase(targetSimpleName)
    }
    false
  }

  private def matchesDefinition(definition: FunctionDefinition, targetClassName: String): Boolean = {
    if (definition == null) {
      return false
    }
    definition match {
      case scalarDef: ScalarFunctionDefinition =>
        targetClassName == scalarDef.getScalarFunction.getClass.getName
      case scalar: ScalarFunction =>
        targetClassName == scalar.getClass.getName
      case _ => false
    }
  }

  private def isProxyScalarFunction(
      call: RexCall,
      targetClassName: String,
      targetSimpleName: String): Boolean = {
    val operator = call.getOperator
    if (matchesOperatorName(operator.getName, targetSimpleName)) {
      return true
    }
    operator match {
      case scalar: ScalarSqlFunction =>
        targetClassName == scalar.scalarFunction.getClass.getName
      case bridging: BridgingSqlFunction =>
        val identifier = bridging.getResolvedFunction.getIdentifier
        val identifierName =
          if (identifier.isPresent) identifier.get.getFunctionName else null
        if (matchesOperatorName(identifierName, targetSimpleName)) {
          return true
        }
        matchesDefinition(bridging.getDefinition, targetClassName)
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

  private def extractProxyFieldIndex(operand: RexNode): Option[Integer] = {
    operand match {
      case inputRef: RexInputRef =>
        Some(inputRef.getIndex)
      case fieldAccess: RexFieldAccess =>
        fieldAccess.getReferenceExpr match {
          case inputRef: RexInputRef => Some(inputRef.getIndex)
          case refCall: RexCall if refCall.getKind == SqlKind.CAST || refCall.getKind == SqlKind.AS =>
            extractProxyFieldIndex(refCall.getOperands.get(0))
          case _ => None
        }
      case call: RexCall =>
        if (call.getKind == SqlKind.CAST || call.getKind == SqlKind.AS) {
          extractProxyFieldIndex(call.getOperands.get(0))
        } else {
          None
        }
      case _ => None
    }
  }

  private def resolveFieldName(inputType: RelDataType, fieldIndex: Integer): Option[String] = {
    if (inputType == null || fieldIndex == null) {
      return None
    }
    val fields = inputType.getFieldList
    if (fields == null) {
      return None
    }
    if (fieldIndex < 0 || fieldIndex >= fields.size()) {
      None
    } else {
      Option(fields.get(fieldIndex).getName)
    }
  }

  private def resolveFieldType(inputType: RelDataType, fieldIndex: Integer): Option[String] = {
    if (inputType == null || fieldIndex == null) {
      return None
    }
    val fields = inputType.getFieldList
    if (fields == null) {
      return None
    }
    if (fieldIndex < 0 || fieldIndex >= fields.size()) {
      None
    } else {
      val logicalType = FlinkTypeFactory.toLogicalType(fields.get(fieldIndex).getType)
      Option(logicalType.asSerializableString)
    }
  }

  private def resolveOperandType(
      operand: RexNode,
      inputType: RelDataType,
      fieldIndex: Integer): Option[String] = {
    operand match {
      case call: RexCall if call.getKind == SqlKind.CAST || call.getKind == SqlKind.AS =>
        Option(FlinkTypeFactory.toLogicalType(call.getType).asSerializableString)
      case fieldAccess: RexFieldAccess =>
        fieldAccess.getReferenceExpr match {
          case refCall: RexCall if refCall.getKind == SqlKind.CAST || refCall.getKind == SqlKind.AS =>
            Option(FlinkTypeFactory.toLogicalType(refCall.getType).asSerializableString)
          case _ =>
            resolveFieldType(inputType, fieldIndex)
        }
      case _ =>
        resolveFieldType(inputType, fieldIndex)
    }
  }

  private class ProxyScalarFunctionRewriter(
      inputType: RelDataType,
      outputType: RelDataType,
      targetClassName: String)
    extends RexShuttle {
    private val targetSimpleName = deriveSimpleName(targetClassName)
    private var proxyFunctionFound = false
    private var proxyConf: String = null
    private var proxyFieldIndex: Integer = null
    private var proxyFieldName: String = null
    private var proxyFunctionClass: String = null
    private var proxyFunctionKind: String = CommonExecCalc.PROXY_FUNCTION_KIND_SCALAR
    private var currentOutputFieldIndex: Int = -1
    private var currentUdfFieldIndexOverride: Integer = null
    private val proxyArgFieldIndices = new java.util.ArrayList[Integer]()
    private val proxyArgFieldNames = new java.util.ArrayList[String]()
    private val proxyArgFieldTypes = new java.util.ArrayList[String]()
    private val proxyResultFieldIndices = new java.util.ArrayList[Integer]()
    private val proxyResultFieldNames = new java.util.ArrayList[String]()
    private val proxyResultFieldTypes = new java.util.ArrayList[String]()
    private val proxyResultUdfFieldIndices = new java.util.ArrayList[Integer]()
    private val proxyResultUdfFieldTypes = new java.util.ArrayList[String]()

    def setCurrentOutputFieldIndex(idx: Int): Unit = {
      currentOutputFieldIndex = idx
    }

    override def visitFieldAccess(fieldAccess: RexFieldAccess): RexNode = {
      val proxyCall = unwrapProxyCall(fieldAccess.getReferenceExpr)
      if (proxyCall == null || fieldAccess.getField == null) {
        return super.visitFieldAccess(fieldAccess)
      }
      currentUdfFieldIndexOverride = fieldAccess.getField.getIndex
      try {
        // Drop the field access; the proxy uses the recorded udfFieldIndex.
        proxyCall.accept(this)
      } finally {
        currentUdfFieldIndexOverride = null
      }
    }

    override def visitCall(call: RexCall): RexNode = {
      if (!isProxyScalarFunction(call, targetClassName, targetSimpleName)) {
        return super.visitCall(call)
      }
      val operands = call.getOperands
      if (operands.isEmpty) {
        throw new TableException("Proxy scalar function requires at least one argument.")
      }
      proxyFunctionFound = true
      proxyConf = mergeProxyConf(proxyConf, extractProxyConf(operands))
      proxyFunctionClass = targetClassName
      if (currentOutputFieldIndex < 0) {
        proxyFunctionKind = CommonExecCalc.PROXY_FUNCTION_KIND_FILTER
      }
      var foundFieldArg = false
      var firstFieldOperand: RexNode = null
      var firstFieldIndex: Integer = null
      val iterator = operands.iterator()
      while (iterator.hasNext) {
        val operand = iterator.next()
        if (!(operand.isInstanceOf[RexLiteral] || operand.getKind == SqlKind.DEFAULT)) {
          if (!isSupportedProxyOperand(operand)) {
            throw new TableException(
              "Proxy scalar function requires column references for all non-literal arguments.")
          }
          extractProxyFieldIndex(operand) match {
            case Some(idx) =>
              proxyArgFieldIndices.add(idx)
              proxyArgFieldNames.add(resolveFieldName(inputType, idx).orNull)
              proxyArgFieldTypes.add(resolveOperandType(operand, inputType, idx).orNull)
              if (proxyFieldIndex == null) {
                proxyFieldIndex = idx
                proxyFieldName = resolveFieldName(inputType, idx).orNull
              }
              if (firstFieldOperand == null) {
                firstFieldOperand = operand
                firstFieldIndex = idx
              }
              foundFieldArg = true
            case None =>
              throw new TableException(
                "Proxy scalar function requires input references as arguments.")
          }
        }
      }
      if (!foundFieldArg) {
        throw new TableException(
          "Proxy scalar function requires at least one column reference argument.")
      }
      if (currentOutputFieldIndex < 0) {
        return call
      }
      val udfReturnType = resolveUdfReturnType(call, currentUdfFieldIndexOverride)
      addResultField(currentUdfFieldIndexOverride, firstFieldIndex, udfReturnType)
      firstFieldOperand.accept(this)
    }

    def hasProxyFunction: Boolean = proxyFunctionFound

    def getProxyConf: String = if (proxyConf == null) "" else proxyConf

    def getProxyFieldIndex: Option[Integer] = Option(proxyFieldIndex)

    def getProxyFieldName: String = if (proxyFieldName == null) "" else proxyFieldName

    def getProxyFunctionClass: String = if (proxyFunctionClass == null) "" else proxyFunctionClass

    def getProxyFunctionKind: String = if (proxyFunctionKind == null) "" else proxyFunctionKind

    def getProxyArgFieldIndices: java.util.List[Integer] = proxyArgFieldIndices

    def getProxyArgFieldNames: java.util.List[String] = proxyArgFieldNames

    def getProxyArgFieldTypes: java.util.List[String] = proxyArgFieldTypes

    def getProxyResultFieldIndices: java.util.List[Integer] = proxyResultFieldIndices

    def getProxyResultFieldNames: java.util.List[String] = proxyResultFieldNames

    def getProxyResultFieldTypes: java.util.List[String] = proxyResultFieldTypes

    def getProxyResultUdfFieldIndices: java.util.List[Integer] = proxyResultUdfFieldIndices

    def getProxyResultUdfFieldTypes: java.util.List[String] = proxyResultUdfFieldTypes

    private def addResultField(
        udfFieldIndex: Integer,
        targetFieldIndex: Integer,
        udfFieldType: String): Unit = {
      if (currentOutputFieldIndex < 0) {
        return
      }
      if (targetFieldIndex == null || targetFieldIndex < 0) {
        return
      }
      if (proxyResultFieldIndices.contains(targetFieldIndex: Integer)) {
        return
      }
      proxyResultFieldIndices.add(targetFieldIndex)
      proxyResultFieldNames.add(resolveFieldName(outputType, currentOutputFieldIndex).orNull)
      proxyResultFieldTypes.add(resolveFieldType(outputType, currentOutputFieldIndex).orNull)
      proxyResultUdfFieldIndices.add(if (udfFieldIndex == null) -1 else udfFieldIndex)
      proxyResultUdfFieldTypes.add(udfFieldType)
    }

    private def unwrapProxyCall(node: RexNode): RexCall = {
      node match {
        case call: RexCall =>
          if (isProxyScalarFunction(call, targetClassName, targetSimpleName)) {
            call
          } else if (call.getKind == SqlKind.CAST || call.getKind == SqlKind.AS) {
            unwrapProxyCall(call.getOperands.get(0))
          } else {
            null
          }
        case _ => null
      }
    }

    private def deriveSimpleName(className: String): String = {
      val lastDot = className.lastIndexOf('.')
      val base = if (lastDot < 0) className else className.substring(lastDot + 1)
      val suffixIndex = base.indexOf('$')
      if (suffixIndex > 0) base.substring(0, suffixIndex) else base
    }

    private def resolveUdfReturnType(call: RexCall, udfFieldIndex: Integer): String = {
      if (call == null) {
        return null
      }
      val relType = call.getType
      if (udfFieldIndex != null && udfFieldIndex >= 0 && relType != null) {
        val fields = relType.getFieldList
        if (fields != null && udfFieldIndex < fields.size()) {
          val fieldType = fields.get(udfFieldIndex).getType
          return FlinkTypeFactory.toLogicalType(fieldType).asSerializableString
        }
      }
      if (relType == null) {
        null
      } else {
        FlinkTypeFactory.toLogicalType(relType).asSerializableString
      }
    }
  }
}
