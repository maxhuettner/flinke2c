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

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.transformations.PhysicalTransformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CalcCodeGenerator;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecCalc;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rex.RexNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/** Stream {@link ExecNode} for Calc. */
@ExecNodeMetadata(
        name = "stream-exec-calc",
        version = 1,
        producedTransformations = CommonExecCalc.CALC_TRANSFORMATION,
        minPlanVersion = FlinkVersion.v1_15,
        minStateVersion = FlinkVersion.v1_15)
public class StreamExecCalc extends CommonExecCalc implements StreamExecNode<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(StreamExecCalc.class);

    public static final String FIELD_NAME_PROXY_CONF = "proxyConf";

    private final @Nullable String proxyConf;

    static {
        final String source =
                String.valueOf(StreamExecCalc.class.getProtectionDomain().getCodeSource());
        LOG.warn("StreamExecCalc class loaded from {}", source);
        System.err.println("StreamExecCalc class loaded from " + source);
    }

    public StreamExecCalc(
            ReadableConfig tableConfig,
            List<RexNode> projection,
            @Nullable RexNode condition,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        this(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecCalc.class),
                ExecNodeContext.newPersistedConfig(StreamExecCalc.class, tableConfig),
                projection,
                condition,
                Collections.singletonList(inputProperty),
                outputType,
                description,
                null);
    }

    public StreamExecCalc(
            ReadableConfig tableConfig,
            List<RexNode> projection,
            @Nullable RexNode condition,
            InputProperty inputProperty,
            RowType outputType,
            String description,
            @Nullable String proxyConf) {
        this(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecCalc.class),
                ExecNodeContext.newPersistedConfig(StreamExecCalc.class, tableConfig),
                projection,
                condition,
                Collections.singletonList(inputProperty),
                outputType,
                description,
                proxyConf);
    }

    @JsonCreator
    public StreamExecCalc(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_PROJECTION) List<RexNode> projection,
            @JsonProperty(FIELD_NAME_CONDITION) @Nullable RexNode condition,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description,
            @JsonProperty(FIELD_NAME_PROXY_CONF) @Nullable String proxyConf) {
        super(
                id,
                context,
                persistedConfig,
                projection,
                condition,
                TableStreamOperator.class,
                true, // retainHeader
                inputProperties,
                outputType,
                description);
        this.proxyConf = proxyConf;
        LOG.info(
                "StreamExecCalc ctor: id={}, projectionSize={}, conditionPresent={}, proxyConfPresent={}",
                id,
                projection.size(),
                condition != null,
                proxyConf != null);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        LOG.info(
                "StreamExecCalc translateToPlanInternal: id={}, projectionSize={}, conditionPresent={}, proxyConfPresent={}",
                getId(),
                projection.size(),
                condition != null,
                proxyConf != null);
        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);

        final ProxyScalarFunctionRewriter rewriter = new ProxyScalarFunctionRewriter();
        final List<RexNode> rewrittenProjection = new ArrayList<>(projection.size());
        for (RexNode node : projection) {
            rewrittenProjection.add(node.accept(rewriter));
        }
        final @Nullable RexNode rewrittenCondition =
                condition == null ? null : condition.accept(rewriter);

        final boolean hasProxyFunction = rewriter.hasProxyFunction();
        final boolean useProxyOperators = hasProxyFunction || proxyConf != null;
        LOG.info(
                "StreamExecCalc proxy rewrite: hasProxyFunction={}, proxyConfPresent={}, useProxyOperators={}",
                hasProxyFunction,
                proxyConf != null,
                useProxyOperators);

        final List<RexNode> effectiveProjection =
                hasProxyFunction ? rewrittenProjection : projection;
        final @Nullable RexNode effectiveCondition =
                hasProxyFunction ? rewrittenCondition : condition;
        final @Nullable String resolvedProxyConf;
        if (useProxyOperators) {
            String proxyConfValue = rewriter.getProxyConf();
            if (proxyConf != null && !proxyConf.isEmpty()) {
                if (proxyConfValue.isEmpty()) {
                    proxyConfValue = proxyConf;
                } else if (!proxyConfValue.equals(proxyConf)) {
                    throw new TableException(
                            "Proxy scalar function requires a single, consistent conf literal.");
                }
            }
            if (proxyConfValue.isEmpty()) {
                proxyConfValue = config.getOptional(PROXY_CONF_OPTION).orElse("");
            }
            if (proxyConfValue.isEmpty()) {
                throw new TableException(
                        "Proxy scalar function requires a TCP conf literal or table.exec.proxy.conf.");
            }
            LOG.info(
                    "Proxy rewrite injecting pre/post operators for scalar UDF: {}, conf={}",
                    CUSTOM_PROXY_FUNCTION_NAME,
                    proxyConfValue);
            resolvedProxyConf = proxyConfValue;
        } else {
            resolvedProxyConf = null;
        }

        final CodeGeneratorContext ctx =
                new CodeGeneratorContext(config, planner.getFlinkContext().getClassLoader())
                        .setOperatorBaseClass(getOperatorBaseClass());

        final CodeGenOperatorFactory<RowData> substituteStreamOperator =
                CalcCodeGenerator.generateCalcOperator(
                        ctx,
                        inputTransform,
                        (RowType) getOutputType(),
                        JavaScalaConversionUtil.toScala(effectiveProjection),
                        JavaScalaConversionUtil.toScala(Optional.ofNullable(effectiveCondition)),
                        isRetainHeader(),
                        getClass().getSimpleName());
        final Transformation<RowData> calcTransform =
                ExecNodeUtil.createOneInputTransformation(
                        inputTransform,
                        createTransformationMeta(CALC_TRANSFORMATION, config),
                        substituteStreamOperator,
                        InternalTypeInfo.of(getOutputType()),
                        inputTransform.getParallelism(),
                        false);
        if (resolvedProxyConf != null) {
            if (config.get(PROXY_CHAIN_ONLY_OPTION) && calcTransform instanceof PhysicalTransformation) {
                ((PhysicalTransformation<?>) calcTransform)
                        .setChainingStrategy(ChainingStrategy.HEAD);
            }
            return createProxyChain(calcTransform, resolvedProxyConf, config);
        }
        return calcTransform;
    }
}
