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
    public static final String FIELD_NAME_PROXY_FIELD_INDEX = "proxyFieldIndex";
    public static final String FIELD_NAME_PROXY_FIELD_NAME = "proxyFieldName";
    public static final String FIELD_NAME_PROXY_FUNCTION_CLASS = "proxyFunctionClass";
    public static final String FIELD_NAME_PROXY_FUNCTION_KIND = "proxyFunctionKind";
    public static final String FIELD_NAME_PROXY_ARG_FIELD_INDICES = "proxyArgFieldIndices";
    public static final String FIELD_NAME_PROXY_ARG_FIELD_NAMES = "proxyArgFieldNames";
    public static final String FIELD_NAME_PROXY_ARG_FIELD_TYPES = "proxyArgFieldTypes";
    public static final String FIELD_NAME_PROXY_RESULT_FIELD_INDICES = "proxyResultFieldIndices";
    public static final String FIELD_NAME_PROXY_RESULT_FIELD_NAMES = "proxyResultFieldNames";
    public static final String FIELD_NAME_PROXY_RESULT_FIELD_TYPES = "proxyResultFieldTypes";
    public static final String FIELD_NAME_PROXY_RESULT_UDF_FIELD_TYPES =
            "proxyResultUdfFieldTypes";
    public static final String FIELD_NAME_PROXY_RESULT_UDF_FIELD_INDICES =
            "proxyResultUdfFieldIndices";

    private final @Nullable String proxyConf;
    private final @Nullable Integer proxyFieldIndex;
    private final @Nullable String proxyFieldName;
    private final @Nullable String proxyFunctionClass;
    private final @Nullable String proxyFunctionKind;
    private final @Nullable List<Integer> proxyArgFieldIndices;
    private final @Nullable List<String> proxyArgFieldNames;
    private final @Nullable List<String> proxyArgFieldTypes;
    private final @Nullable List<Integer> proxyResultFieldIndices;
    private final @Nullable List<String> proxyResultFieldNames;
    private final @Nullable List<String> proxyResultFieldTypes;
    private final @Nullable List<String> proxyResultUdfFieldTypes;
    private final @Nullable List<Integer> proxyResultUdfFieldIndices;

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
                tableConfig,
                projection,
                condition,
                inputProperty,
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
                proxyConf,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null);
    }

    public StreamExecCalc(
            ReadableConfig tableConfig,
            List<RexNode> projection,
            @Nullable RexNode condition,
            InputProperty inputProperty,
            RowType outputType,
            String description,
            @Nullable String proxyConf,
            @Nullable Integer proxyFieldIndex,
            @Nullable String proxyFieldName,
            @Nullable String proxyFunctionClass,
            @Nullable String proxyFunctionKind,
            @Nullable List<Integer> proxyArgFieldIndices,
            @Nullable List<String> proxyArgFieldNames,
            @Nullable List<String> proxyArgFieldTypes,
            @Nullable List<Integer> proxyResultFieldIndices,
            @Nullable List<String> proxyResultFieldNames,
            @Nullable List<String> proxyResultFieldTypes,
            @Nullable List<String> proxyResultUdfFieldTypes,
            @Nullable List<Integer> proxyResultUdfFieldIndices) {
        this(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecCalc.class),
                ExecNodeContext.newPersistedConfig(StreamExecCalc.class, tableConfig),
                projection,
                condition,
                Collections.singletonList(inputProperty),
                outputType,
                description,
                proxyConf,
                proxyFieldIndex,
                proxyFieldName,
                proxyFunctionClass,
                proxyFunctionKind,
                proxyArgFieldIndices,
                proxyArgFieldNames,
                proxyArgFieldTypes,
                proxyResultFieldIndices,
                proxyResultFieldNames,
                proxyResultFieldTypes,
                proxyResultUdfFieldTypes,
                proxyResultUdfFieldIndices);
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
            @JsonProperty(FIELD_NAME_PROXY_CONF) @Nullable String proxyConf,
            @JsonProperty(FIELD_NAME_PROXY_FIELD_INDEX) @Nullable Integer proxyFieldIndex,
            @JsonProperty(FIELD_NAME_PROXY_FIELD_NAME) @Nullable String proxyFieldName,
            @JsonProperty(FIELD_NAME_PROXY_FUNCTION_CLASS) @Nullable String proxyFunctionClass,
            @JsonProperty(FIELD_NAME_PROXY_FUNCTION_KIND) @Nullable String proxyFunctionKind,
            @JsonProperty(FIELD_NAME_PROXY_ARG_FIELD_INDICES)
                    @Nullable List<Integer> proxyArgFieldIndices,
            @JsonProperty(FIELD_NAME_PROXY_ARG_FIELD_NAMES)
                    @Nullable List<String> proxyArgFieldNames,
            @JsonProperty(FIELD_NAME_PROXY_ARG_FIELD_TYPES)
                    @Nullable List<String> proxyArgFieldTypes,
            @JsonProperty(FIELD_NAME_PROXY_RESULT_FIELD_INDICES)
                    @Nullable List<Integer> proxyResultFieldIndices,
            @JsonProperty(FIELD_NAME_PROXY_RESULT_FIELD_NAMES)
                    @Nullable List<String> proxyResultFieldNames,
            @JsonProperty(FIELD_NAME_PROXY_RESULT_FIELD_TYPES)
                    @Nullable List<String> proxyResultFieldTypes,
            @JsonProperty(FIELD_NAME_PROXY_RESULT_UDF_FIELD_TYPES)
                    @Nullable List<String> proxyResultUdfFieldTypes,
            @JsonProperty(FIELD_NAME_PROXY_RESULT_UDF_FIELD_INDICES)
                    @Nullable List<Integer> proxyResultUdfFieldIndices) {
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
        this.proxyFieldIndex = proxyFieldIndex;
        this.proxyFieldName = proxyFieldName;
        this.proxyFunctionClass = proxyFunctionClass;
        this.proxyFunctionKind = proxyFunctionKind;
        this.proxyArgFieldIndices = proxyArgFieldIndices;
        this.proxyArgFieldNames = proxyArgFieldNames;
        this.proxyArgFieldTypes = proxyArgFieldTypes;
        this.proxyResultFieldIndices = proxyResultFieldIndices;
        this.proxyResultFieldNames = proxyResultFieldNames;
        this.proxyResultFieldTypes = proxyResultFieldTypes;
        this.proxyResultUdfFieldTypes = proxyResultUdfFieldTypes;
        this.proxyResultUdfFieldIndices = proxyResultUdfFieldIndices;
        LOG.info(
                "StreamExecCalc ctor: id={}, projectionSize={}, conditionPresent={}, proxyConfPresent={}, proxyFieldIndex={}, proxyFieldName={}, proxyFunctionClass={}, proxyFunctionKind={}, proxyArgFieldIndices={}, proxyResultFieldIndices={}",
                id,
                projection.size(),
                condition != null,
                proxyConf != null,
                proxyFieldIndex,
                proxyFieldName,
                proxyFunctionClass,
                proxyFunctionKind,
                proxyArgFieldIndices,
                proxyResultFieldIndices);
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
        final RowType inputRowType = extractRowType(inputTransform);
        final RowType outputRowType = (RowType) getOutputType();

        final String resolvedProxyFunctionClass =
                proxyFunctionClass != null && !proxyFunctionClass.isEmpty()
                        ? proxyFunctionClass
                        : config.getOptional(PROXY_FUNCTION_CLASS_OPTION)
                                .orElse(CUSTOM_PROXY_FUNCTION_CLASS_NAME);
        final String resolvedProxyFunctionKind =
                proxyFunctionKind != null && !proxyFunctionKind.isEmpty()
                        ? proxyFunctionKind
                        : PROXY_FUNCTION_KIND_SCALAR;

        final ProxyScalarFunctionRewriter rewriter =
                new ProxyScalarFunctionRewriter(
                        resolvedProxyFunctionClass, inputRowType, outputRowType);
        final List<RexNode> rewrittenProjection = new ArrayList<>(projection.size());
        for (int i = 0; i < projection.size(); i++) {
            rewriter.setCurrentOutputFieldIndex(i);
            rewrittenProjection.add(projection.get(i).accept(rewriter));
        }
        rewriter.setCurrentOutputFieldIndex(-1);
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
        @Nullable List<String> resolvedResultFieldTypes = null;
        @Nullable List<Integer> resolvedResultFieldIndices = null;
        if (useProxyOperators) {
            String proxyConfValue = rewriter.getProxyConf();

            final List<Integer> resolvedArgFieldIndices =
                    proxyArgFieldIndices != null
                            ? proxyArgFieldIndices
                            : rewriter.getProxyArgFieldIndices();
            final List<String> resolvedArgFieldNames =
                    proxyArgFieldNames != null
                            ? proxyArgFieldNames
                            : rewriter.getProxyArgFieldNames();
            final List<String> resolvedArgFieldTypes =
                    proxyArgFieldTypes != null
                            ? proxyArgFieldTypes
                            : rewriter.getProxyArgFieldTypes();
            List<String> resolvedResultFieldNames =
                    proxyResultFieldNames != null
                            ? proxyResultFieldNames
                            : rewriter.getProxyResultFieldNames();
            resolvedResultFieldTypes =
                    proxyResultFieldTypes != null
                            ? proxyResultFieldTypes
                            : rewriter.getProxyResultFieldTypes();
            resolvedResultFieldIndices =
                    proxyResultFieldIndices != null
                            ? proxyResultFieldIndices
                            : rewriter.getProxyResultFieldIndices();
            if (resolvedResultFieldIndices != null && !resolvedResultFieldIndices.isEmpty()) {
                // Ensure result metadata is aligned with the current output row type.
                // This guards against missing or stale result type/name info from the rewriter.
                final List<String> outputFieldTypes =
                        resolveFieldTypesFromOutput(outputRowType, resolvedResultFieldIndices);
                if (resolvedResultFieldTypes == null
                        || resolvedResultFieldTypes.size() != resolvedResultFieldIndices.size()
                        || containsNullOrEmpty(resolvedResultFieldTypes)
                        || !resolvedResultFieldTypes.equals(outputFieldTypes)) {
                    resolvedResultFieldTypes = outputFieldTypes;
                }
                final List<String> outputFieldNames =
                        resolveFieldNamesFromOutput(outputRowType, resolvedResultFieldIndices);
                if (resolvedResultFieldNames == null
                        || resolvedResultFieldNames.size() != resolvedResultFieldIndices.size()
                        || containsNullOrEmpty(resolvedResultFieldNames)
                        || !resolvedResultFieldNames.equals(outputFieldNames)) {
                    resolvedResultFieldNames = outputFieldNames;
                }
            }
            final List<String> resolvedResultUdfFieldTypes =
                    proxyResultUdfFieldTypes != null
                            ? proxyResultUdfFieldTypes
                            : rewriter.getProxyResultUdfFieldTypes();
            final List<Integer> resolvedResultUdfFieldIndices =
                    proxyResultUdfFieldIndices != null
                            ? proxyResultUdfFieldIndices
                            : rewriter.getProxyResultUdfFieldIndices();
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
            if (proxyFieldIndex != null || (proxyFieldName != null && !proxyFieldName.isEmpty())) {
                proxyConfValue =
                        appendProxyField(proxyConfValue, proxyFieldIndex, proxyFieldName);
            }
            proxyConfValue =
                    appendProxyFunctionMetadata(
                            proxyConfValue, resolvedProxyFunctionClass, resolvedProxyFunctionKind);
            proxyConfValue =
                    appendProxyFunctionArgsMetadata(
                            proxyConfValue,
                            resolvedArgFieldIndices,
                            resolvedArgFieldNames,
                            resolvedArgFieldTypes);
            proxyConfValue =
                    appendProxyFunctionResultMetadata(
                            proxyConfValue,
                            resolvedResultFieldIndices,
                            resolvedResultFieldNames,
                            resolvedResultFieldTypes,
                            resolvedResultUdfFieldTypes,
                            resolvedResultUdfFieldIndices);
            LOG.info(
                    "Proxy rewrite injecting pre/post operators: functionClass={}, functionKind={}, argFieldIndices={}, resultFieldIndices={}, conf={}",
                    resolvedProxyFunctionClass,
                    resolvedProxyFunctionKind,
                    resolvedArgFieldIndices,
                    resolvedResultFieldIndices,
                    proxyConfValue);
            resolvedProxyConf = proxyConfValue;
        } else {
            resolvedProxyConf = null;
        }

        final RowType proxyOutputRowType =
                resolvedProxyConf != null
                        ? applyResultTypes(
                                inputRowType,
                                resolvedResultFieldIndices,
                                resolvedResultFieldTypes,
                                planner.getFlinkContext().getClassLoader())
                        : inputRowType;

        final Transformation<RowData> proxyInputTransform =
                resolvedProxyConf != null
                        ? createProxyChain(inputTransform, resolvedProxyConf, config, proxyOutputRowType)
                        : inputTransform;

        final CodeGeneratorContext ctx =
                new CodeGeneratorContext(config, planner.getFlinkContext().getClassLoader())
                        .setOperatorBaseClass(getOperatorBaseClass());

        final CodeGenOperatorFactory<RowData> substituteStreamOperator =
                CalcCodeGenerator.generateCalcOperator(
                        ctx,
                        proxyInputTransform,
                        (RowType) getOutputType(),
                        JavaScalaConversionUtil.toScala(effectiveProjection),
                        JavaScalaConversionUtil.toScala(Optional.ofNullable(effectiveCondition)),
                        isRetainHeader(),
                        getClass().getSimpleName());
        final Transformation<RowData> calcTransform =
                ExecNodeUtil.createOneInputTransformation(
                        proxyInputTransform,
                        createTransformationMeta(CALC_TRANSFORMATION, config),
                        substituteStreamOperator,
                        InternalTypeInfo.of(getOutputType()),
                        proxyInputTransform.getParallelism(),
                        false);
        return calcTransform;
    }

    private static String appendProxyField(
            String conf, @Nullable Integer fieldIndex, @Nullable String fieldName) {
        String result = conf;
        if (fieldIndex != null && !containsConfKey(result, "calcfieldindex")) {
            result = appendConfValue(result, "calcFieldIndex", String.valueOf(fieldIndex));
        }
        if (fieldName != null && !fieldName.isEmpty()
                && !containsConfKey(result, "calcfieldname")) {
            result = appendConfValue(result, "calcFieldName", fieldName);
        }
        return result;
    }

    private static boolean containsNullOrEmpty(@Nullable List<String> values) {
        if (values == null || values.isEmpty()) {
            return true;
        }
        for (String value : values) {
            if (value == null || value.trim().isEmpty()) {
                return true;
            }
        }
        return false;
    }

    private static List<String> resolveFieldTypesFromOutput(
            RowType outputRowType, List<Integer> fieldIndices) {
        if (fieldIndices == null || fieldIndices.isEmpty()) {
            return Collections.emptyList();
        }
        final List<RowType.RowField> fields = outputRowType.getFields();
        final List<String> types = new ArrayList<>(fieldIndices.size());
        for (Integer idx : fieldIndices) {
            if (idx == null || idx < 0 || idx >= fields.size()) {
                types.add(null);
            } else {
                types.add(fields.get(idx).getType().asSerializableString());
            }
        }
        return types;
    }

    private static List<String> resolveFieldNamesFromOutput(
            RowType outputRowType, List<Integer> fieldIndices) {
        if (fieldIndices == null || fieldIndices.isEmpty()) {
            return Collections.emptyList();
        }
        final List<RowType.RowField> fields = outputRowType.getFields();
        final List<String> names = new ArrayList<>(fieldIndices.size());
        for (Integer idx : fieldIndices) {
            if (idx == null || idx < 0 || idx >= fields.size()) {
                names.add(null);
            } else {
                names.add(fields.get(idx).getName());
            }
        }
        return names;
    }
}
