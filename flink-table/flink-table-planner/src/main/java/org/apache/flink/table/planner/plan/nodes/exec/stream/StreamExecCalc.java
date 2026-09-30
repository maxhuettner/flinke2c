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

    public static final String FIELD_NAME_EXTERNAL_RUNTIME_CONF = "externalRuntimeConf";
    public static final String FIELD_NAME_EXTERNAL_RUNTIME_FIELD_INDEX = "externalRuntimeFieldIndex";
    public static final String FIELD_NAME_EXTERNAL_RUNTIME_FIELD_NAME = "externalRuntimeFieldName";
    public static final String FIELD_NAME_EXTERNAL_RUNTIME_FUNCTION_CLASS = "externalRuntimeFunctionClass";
    public static final String FIELD_NAME_EXTERNAL_RUNTIME_FUNCTION_KIND = "externalRuntimeFunctionKind";
    public static final String FIELD_NAME_EXTERNAL_RUNTIME_ARG_FIELD_INDICES = "externalRuntimeArgFieldIndices";
    public static final String FIELD_NAME_EXTERNAL_RUNTIME_ARG_FIELD_NAMES = "externalRuntimeArgFieldNames";
    public static final String FIELD_NAME_EXTERNAL_RUNTIME_ARG_FIELD_TYPES = "externalRuntimeArgFieldTypes";
    public static final String FIELD_NAME_EXTERNAL_RUNTIME_RESULT_FIELD_INDICES = "externalRuntimeResultFieldIndices";
    public static final String FIELD_NAME_EXTERNAL_RUNTIME_RESULT_FIELD_NAMES = "externalRuntimeResultFieldNames";
    public static final String FIELD_NAME_EXTERNAL_RUNTIME_RESULT_FIELD_TYPES = "externalRuntimeResultFieldTypes";
    public static final String FIELD_NAME_EXTERNAL_RUNTIME_RESULT_UDF_FIELD_TYPES =
            "externalRuntimeResultUdfFieldTypes";
    public static final String FIELD_NAME_EXTERNAL_RUNTIME_RESULT_UDF_FIELD_INDICES =
            "externalRuntimeResultUdfFieldIndices";

    private final @Nullable String externalRuntimeConf;
    private final @Nullable Integer externalRuntimeFieldIndex;
    private final @Nullable String externalRuntimeFieldName;
    private final @Nullable String externalRuntimeFunctionClass;
    private final @Nullable String externalRuntimeFunctionKind;
    private final @Nullable List<Integer> externalRuntimeArgFieldIndices;
    private final @Nullable List<String> externalRuntimeArgFieldNames;
    private final @Nullable List<String> externalRuntimeArgFieldTypes;
    private final @Nullable List<Integer> externalRuntimeResultFieldIndices;
    private final @Nullable List<String> externalRuntimeResultFieldNames;
    private final @Nullable List<String> externalRuntimeResultFieldTypes;
    private final @Nullable List<String> externalRuntimeResultUdfFieldTypes;
    private final @Nullable List<Integer> externalRuntimeResultUdfFieldIndices;

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
            @Nullable String externalRuntimeConf) {
        this(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecCalc.class),
                ExecNodeContext.newPersistedConfig(StreamExecCalc.class, tableConfig),
                projection,
                condition,
                Collections.singletonList(inputProperty),
                outputType,
                description,
                externalRuntimeConf,
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
            @Nullable String externalRuntimeConf,
            @Nullable Integer externalRuntimeFieldIndex,
            @Nullable String externalRuntimeFieldName,
            @Nullable String externalRuntimeFunctionClass,
            @Nullable String externalRuntimeFunctionKind,
            @Nullable List<Integer> externalRuntimeArgFieldIndices,
            @Nullable List<String> externalRuntimeArgFieldNames,
            @Nullable List<String> externalRuntimeArgFieldTypes,
            @Nullable List<Integer> externalRuntimeResultFieldIndices,
            @Nullable List<String> externalRuntimeResultFieldNames,
            @Nullable List<String> externalRuntimeResultFieldTypes,
            @Nullable List<String> externalRuntimeResultUdfFieldTypes,
            @Nullable List<Integer> externalRuntimeResultUdfFieldIndices) {
        this(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecCalc.class),
                ExecNodeContext.newPersistedConfig(StreamExecCalc.class, tableConfig),
                projection,
                condition,
                Collections.singletonList(inputProperty),
                outputType,
                description,
                externalRuntimeConf,
                externalRuntimeFieldIndex,
                externalRuntimeFieldName,
                externalRuntimeFunctionClass,
                externalRuntimeFunctionKind,
                externalRuntimeArgFieldIndices,
                externalRuntimeArgFieldNames,
                externalRuntimeArgFieldTypes,
                externalRuntimeResultFieldIndices,
                externalRuntimeResultFieldNames,
                externalRuntimeResultFieldTypes,
                externalRuntimeResultUdfFieldTypes,
                externalRuntimeResultUdfFieldIndices);
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
            @JsonProperty(FIELD_NAME_EXTERNAL_RUNTIME_CONF) @Nullable String externalRuntimeConf,
            @JsonProperty(FIELD_NAME_EXTERNAL_RUNTIME_FIELD_INDEX) @Nullable Integer externalRuntimeFieldIndex,
            @JsonProperty(FIELD_NAME_EXTERNAL_RUNTIME_FIELD_NAME) @Nullable String externalRuntimeFieldName,
            @JsonProperty(FIELD_NAME_EXTERNAL_RUNTIME_FUNCTION_CLASS) @Nullable String externalRuntimeFunctionClass,
            @JsonProperty(FIELD_NAME_EXTERNAL_RUNTIME_FUNCTION_KIND) @Nullable String externalRuntimeFunctionKind,
            @JsonProperty(FIELD_NAME_EXTERNAL_RUNTIME_ARG_FIELD_INDICES)
                    @Nullable List<Integer> externalRuntimeArgFieldIndices,
            @JsonProperty(FIELD_NAME_EXTERNAL_RUNTIME_ARG_FIELD_NAMES)
                    @Nullable List<String> externalRuntimeArgFieldNames,
            @JsonProperty(FIELD_NAME_EXTERNAL_RUNTIME_ARG_FIELD_TYPES)
                    @Nullable List<String> externalRuntimeArgFieldTypes,
            @JsonProperty(FIELD_NAME_EXTERNAL_RUNTIME_RESULT_FIELD_INDICES)
                    @Nullable List<Integer> externalRuntimeResultFieldIndices,
            @JsonProperty(FIELD_NAME_EXTERNAL_RUNTIME_RESULT_FIELD_NAMES)
                    @Nullable List<String> externalRuntimeResultFieldNames,
            @JsonProperty(FIELD_NAME_EXTERNAL_RUNTIME_RESULT_FIELD_TYPES)
                    @Nullable List<String> externalRuntimeResultFieldTypes,
            @JsonProperty(FIELD_NAME_EXTERNAL_RUNTIME_RESULT_UDF_FIELD_TYPES)
                    @Nullable List<String> externalRuntimeResultUdfFieldTypes,
            @JsonProperty(FIELD_NAME_EXTERNAL_RUNTIME_RESULT_UDF_FIELD_INDICES)
                    @Nullable List<Integer> externalRuntimeResultUdfFieldIndices) {
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
        this.externalRuntimeConf = externalRuntimeConf;
        this.externalRuntimeFieldIndex = externalRuntimeFieldIndex;
        this.externalRuntimeFieldName = externalRuntimeFieldName;
        this.externalRuntimeFunctionClass = externalRuntimeFunctionClass;
        this.externalRuntimeFunctionKind = externalRuntimeFunctionKind;
        this.externalRuntimeArgFieldIndices = externalRuntimeArgFieldIndices;
        this.externalRuntimeArgFieldNames = externalRuntimeArgFieldNames;
        this.externalRuntimeArgFieldTypes = externalRuntimeArgFieldTypes;
        this.externalRuntimeResultFieldIndices = externalRuntimeResultFieldIndices;
        this.externalRuntimeResultFieldNames = externalRuntimeResultFieldNames;
        this.externalRuntimeResultFieldTypes = externalRuntimeResultFieldTypes;
        this.externalRuntimeResultUdfFieldTypes = externalRuntimeResultUdfFieldTypes;
        this.externalRuntimeResultUdfFieldIndices = externalRuntimeResultUdfFieldIndices;
        LOG.info(
                "StreamExecCalc ctor: id={}, projectionSize={}, conditionPresent={}, externalRuntimeConfPresent={}, externalRuntimeFieldIndex={}, externalRuntimeFieldName={}, externalRuntimeFunctionClass={}, externalRuntimeFunctionKind={}, externalRuntimeArgFieldIndices={}, externalRuntimeResultFieldIndices={}",
                id,
                projection.size(),
                condition != null,
                externalRuntimeConf != null,
                externalRuntimeFieldIndex,
                externalRuntimeFieldName,
                externalRuntimeFunctionClass,
                externalRuntimeFunctionKind,
                externalRuntimeArgFieldIndices,
                externalRuntimeResultFieldIndices);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        LOG.info(
                "StreamExecCalc translateToPlanInternal: id={}, projectionSize={}, conditionPresent={}, externalRuntimeConfPresent={}",
                getId(),
                projection.size(),
                condition != null,
                externalRuntimeConf != null);
        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);
        final RowType inputRowType = extractRowType(inputTransform);
        final RowType outputRowType = (RowType) getOutputType();

        // GPU runtime takes priority, same as CommonExecCalc's shared path: if configured and
        // this Calc actually calls the target UDF, replace it with a single GpuRuntimeOperator
        // instead of falling into the external-runtime PRE/POST rewrite below. StreamExecCalc
        // fully overrides CommonExecCalc.translateToPlanInternal (to support the compiled-plan
        // persisted external-runtime fields above), so this check has to be duplicated here
        // rather than inherited.
        // GPU runtime options are custom planner options and are not included in the filtered
        // persisted ExecNodeConfig. Use the live planner configuration here; external-runtime
        // metadata continues to come from the persisted exec-node fields below.
        final ReadableConfig runtimeConfig = planner.getTableConfig();
        final List<String> gpuRuntimeFunctionClasses =
                CommonExecCalc.resolveGpuRuntimeFunctionClasses(runtimeConfig);
        if (!gpuRuntimeFunctionClasses.isEmpty()) {
            final Transformation<RowData> gpuResult =
                    translateWithGpuRuntime(
                            planner,
                            config,
                            inputTransform,
                            inputRowType,
                            outputRowType,
                            gpuRuntimeFunctionClasses);
            if (gpuResult != null) {
                return gpuResult;
            }
        }

        final List<String> externalRuntimeFunctionClasses =
                externalRuntimeFunctionClass != null && !externalRuntimeFunctionClass.isEmpty()
                        ? List.of(externalRuntimeFunctionClass)
                        : CommonExecCalc.resolveExternalRuntimeFunctionClasses(config);
        final String resolvedExternalRuntimeFunctionKind =
                externalRuntimeFunctionKind != null && !externalRuntimeFunctionKind.isEmpty()
                        ? externalRuntimeFunctionKind
                        : EXTERNAL_RUNTIME_FUNCTION_KIND_SCALAR;

        final ExternalRuntimeScalarFunctionRewriter rewriter =
                new ExternalRuntimeScalarFunctionRewriter(
                        externalRuntimeFunctionClasses, inputRowType, outputRowType);
        final List<RexNode> rewrittenProjection = new ArrayList<>(projection.size());
        for (int i = 0; i < projection.size(); i++) {
            rewriter.setCurrentOutputFieldIndex(i);
            rewrittenProjection.add(projection.get(i).accept(rewriter));
        }
        rewriter.setCurrentOutputFieldIndex(-1);
        final @Nullable RexNode rewrittenCondition =
                condition == null ? null : condition.accept(rewriter);

        final boolean hasExternalRuntimeFunction = rewriter.hasExternalRuntimeFunction();
        final boolean externalRuntimeFilter =
                hasExternalRuntimeFunction
                        && CommonExecCalc.EXTERNAL_RUNTIME_FUNCTION_KIND_FILTER.equals(
                                rewriter.getExternalRuntimeFunctionKind());
        final boolean useExternalRuntimeOperators = hasExternalRuntimeFunction || externalRuntimeConf != null;
        LOG.info(
                "StreamExecCalc external runtime rewrite: hasExternalRuntimeFunction={}, externalRuntimeConfPresent={}, useExternalRuntimeOperators={}",
                hasExternalRuntimeFunction,
                externalRuntimeConf != null,
                useExternalRuntimeOperators);

        final List<RexNode> effectiveProjection =
                hasExternalRuntimeFunction ? rewrittenProjection : projection;
        final @Nullable RexNode effectiveCondition =
                externalRuntimeFilter ? null : (hasExternalRuntimeFunction ? rewrittenCondition : condition);
        final @Nullable String resolvedExternalRuntimeConf;
        @Nullable List<String> resolvedResultFieldTypes = null;
        @Nullable List<Integer> resolvedResultFieldIndices = null;
        if (useExternalRuntimeOperators) {
            String externalRuntimeConfValue = rewriter.getExternalRuntimeConf();

            final List<Integer> resolvedArgFieldIndices =
                    externalRuntimeArgFieldIndices != null
                            ? externalRuntimeArgFieldIndices
                            : rewriter.getExternalRuntimeArgFieldIndices();
            final List<String> resolvedArgFieldNames =
                    externalRuntimeArgFieldNames != null
                            ? externalRuntimeArgFieldNames
                            : rewriter.getExternalRuntimeArgFieldNames();
            final List<String> resolvedArgFieldTypes =
                    externalRuntimeArgFieldTypes != null
                            ? externalRuntimeArgFieldTypes
                            : rewriter.getExternalRuntimeArgFieldTypes();
            List<String> resolvedResultFieldNames =
                    externalRuntimeResultFieldNames != null
                            ? externalRuntimeResultFieldNames
                            : rewriter.getExternalRuntimeResultFieldNames();
            resolvedResultFieldTypes =
                    externalRuntimeResultFieldTypes != null
                            ? externalRuntimeResultFieldTypes
                            : rewriter.getExternalRuntimeResultFieldTypes();
            resolvedResultFieldIndices =
                    externalRuntimeResultFieldIndices != null
                            ? externalRuntimeResultFieldIndices
                            : rewriter.getExternalRuntimeResultFieldIndices();
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
                    externalRuntimeResultUdfFieldTypes != null
                            ? externalRuntimeResultUdfFieldTypes
                            : rewriter.getExternalRuntimeResultUdfFieldTypes();
            final List<Integer> resolvedResultUdfFieldIndices =
                    externalRuntimeResultUdfFieldIndices != null
                            ? externalRuntimeResultUdfFieldIndices
                            : rewriter.getExternalRuntimeResultUdfFieldIndices();
            if (externalRuntimeConf != null && !externalRuntimeConf.isEmpty()) {
                if (externalRuntimeConfValue.isEmpty()) {
                    externalRuntimeConfValue = externalRuntimeConf;
                } else if (!externalRuntimeConfValue.equals(externalRuntimeConf)) {
                    throw new TableException(
                            "External runtime function requires a single, consistent conf literal.");
                }
            }
            if (externalRuntimeConfValue.isEmpty()) {
                externalRuntimeConfValue =
                        CommonExecCalc.resolveExternalRuntimeConf(config, rewriter.getExternalRuntimeFunctionClass());
            }
            if (externalRuntimeConfValue.isEmpty()) {
                throw new TableException(
                        "External runtime function requires a TCP conf literal, "
                                + "table.exec.external-runtime.conf, "
                                + "or table.exec.external-runtime.conf.<functionClass>.");
            }
            if (externalRuntimeFieldIndex != null || (externalRuntimeFieldName != null && !externalRuntimeFieldName.isEmpty())) {
                externalRuntimeConfValue =
                        appendExternalRuntimeField(externalRuntimeConfValue, externalRuntimeFieldIndex, externalRuntimeFieldName);
            }
            final String resolvedExternalRuntimeFunctionClass = rewriter.getExternalRuntimeFunctionClass();
            externalRuntimeConfValue =
                    appendExternalRuntimeFunctionMetadata(
                            externalRuntimeConfValue, resolvedExternalRuntimeFunctionClass, resolvedExternalRuntimeFunctionKind);
            externalRuntimeConfValue =
                    appendExternalRuntimeFunctionArgsMetadata(
                            externalRuntimeConfValue,
                            resolvedArgFieldIndices,
                            resolvedArgFieldNames,
                            resolvedArgFieldTypes);
            externalRuntimeConfValue =
                    appendExternalRuntimeFunctionResultMetadata(
                            externalRuntimeConfValue,
                            resolvedResultFieldIndices,
                            resolvedResultFieldNames,
                            resolvedResultFieldTypes,
                            resolvedResultUdfFieldTypes,
                            resolvedResultUdfFieldIndices);
            LOG.info(
                    "External runtime rewrite injecting pre/post operators: functionClass={}, functionKind={}, argFieldIndices={}, resultFieldIndices={}, conf={}",
                    resolvedExternalRuntimeFunctionClass,
                    resolvedExternalRuntimeFunctionKind,
                    resolvedArgFieldIndices,
                    resolvedResultFieldIndices,
                    externalRuntimeConfValue);
            resolvedExternalRuntimeConf = externalRuntimeConfValue;
        } else {
            resolvedExternalRuntimeConf = null;
        }

        final RowType externalRuntimeOutputRowType =
                resolvedExternalRuntimeConf != null
                        ? applyResultTypes(
                                inputRowType,
                                resolvedResultFieldIndices,
                                resolvedResultFieldTypes,
                                planner.getFlinkContext().getClassLoader())
                        : inputRowType;

        final Transformation<RowData> externalRuntimeInputTransform =
                resolvedExternalRuntimeConf != null
                        ? createExternalRuntimeChain(
                                inputTransform, resolvedExternalRuntimeConf, config, externalRuntimeOutputRowType)
                        : inputTransform;

        final CodeGeneratorContext ctx =
                new CodeGeneratorContext(config, planner.getFlinkContext().getClassLoader())
                        .setOperatorBaseClass(getOperatorBaseClass());

        final CodeGenOperatorFactory<RowData> substituteStreamOperator =
                CalcCodeGenerator.generateCalcOperator(
                        ctx,
                        externalRuntimeInputTransform,
                        (RowType) getOutputType(),
                        JavaScalaConversionUtil.toScala(effectiveProjection),
                        JavaScalaConversionUtil.toScala(Optional.ofNullable(effectiveCondition)),
                        isRetainHeader(),
                        getClass().getSimpleName());
        final @Nullable Integer calcParallelism =
                CommonExecCalc.resolveExternalRuntimeCalcParallelism(config, projection, condition);
        final int configuredParallelism;
        final boolean parallelismConfigured;
        if (calcParallelism != null && calcParallelism > 0) {
            configuredParallelism = calcParallelism;
            parallelismConfigured = true;
        } else {
            configuredParallelism = externalRuntimeInputTransform.getParallelism();
            parallelismConfigured = false;
        }
        final Transformation<RowData> calcTransform =
                ExecNodeUtil.createOneInputTransformation(
                        externalRuntimeInputTransform,
                        createTransformationMeta(CALC_TRANSFORMATION, config),
                        substituteStreamOperator,
                        InternalTypeInfo.of(getOutputType()),
                        configuredParallelism,
                        parallelismConfigured);
        return calcTransform;
    }

    /**
     * Mirrors {@code CommonExecCalc.translateWithRuntimeChain}'s gpu branch: rewrites {@link
     * #projection}/{@link #condition} against {@code gpuRuntimeFunctionClasses} and, if matched,
     * injects a single GPU runtime operator ahead of the codegen'd Calc. Returns {@code null} if
     * nothing matched, so the caller falls through to the external-runtime/plain-Calc path.
     */
    private @Nullable Transformation<RowData> translateWithGpuRuntime(
            PlannerBase planner,
            ExecNodeConfig config,
            Transformation<RowData> inputTransform,
            RowType inputRowType,
            RowType outputRowType,
            List<String> gpuRuntimeFunctionClasses) {
        final ExternalRuntimeScalarFunctionRewriter rewriter =
                new ExternalRuntimeScalarFunctionRewriter(gpuRuntimeFunctionClasses, inputRowType, outputRowType);
        final List<RexNode> rewrittenProjection = new ArrayList<>(projection.size());
        for (int i = 0; i < projection.size(); i++) {
            rewriter.setCurrentOutputFieldIndex(i);
            rewrittenProjection.add(projection.get(i).accept(rewriter));
        }
        rewriter.setCurrentOutputFieldIndex(-1);
        final @Nullable RexNode rewrittenCondition =
                condition == null ? null : condition.accept(rewriter);

        if (!rewriter.hasExternalRuntimeFunction()) {
            return null;
        }

        String gpuRuntimeConf = rewriter.getExternalRuntimeConf();
        if (gpuRuntimeConf.isEmpty()) {
            gpuRuntimeConf =
                    CommonExecCalc.resolveGpuRuntimeConf(
                            planner.getTableConfig(), rewriter.getExternalRuntimeFunctionClass());
        }
        if (gpuRuntimeConf.isEmpty()) {
            throw new TableException(
                    "GPU runtime function requires a conf literal, table.exec.gpu-runtime.conf, "
                            + "or table.exec.gpu-runtime.conf.<functionClass>.");
        }
        gpuRuntimeConf =
                appendExternalRuntimeFunctionMetadata(
                        gpuRuntimeConf,
                        rewriter.getExternalRuntimeFunctionClass(),
                        rewriter.getExternalRuntimeFunctionKind());
        gpuRuntimeConf =
                appendExternalRuntimeFunctionArgsMetadata(
                        gpuRuntimeConf,
                        rewriter.getExternalRuntimeArgFieldIndices(),
                        rewriter.getExternalRuntimeArgFieldNames(),
                        rewriter.getExternalRuntimeArgFieldTypes());
        gpuRuntimeConf =
                appendExternalRuntimeFunctionResultMetadata(
                        gpuRuntimeConf,
                        rewriter.getExternalRuntimeResultFieldIndices(),
                        rewriter.getExternalRuntimeResultFieldNames(),
                        rewriter.getExternalRuntimeResultFieldTypes(),
                        rewriter.getExternalRuntimeResultUdfFieldTypes(),
                        rewriter.getExternalRuntimeResultUdfFieldIndices());
        LOG.info(
                "GPU runtime rewrite injecting operator: functionClass={}, functionKind={}, resultFieldIndices={}",
                rewriter.getExternalRuntimeFunctionClass(),
                rewriter.getExternalRuntimeFunctionKind(),
                rewriter.getExternalRuntimeResultFieldIndices());

        final RowType gpuRuntimeOutputRowType =
                applyResultTypes(
                        inputRowType,
                        rewriter.getExternalRuntimeResultFieldIndices(),
                        rewriter.getExternalRuntimeResultFieldTypes(),
                        planner.getFlinkContext().getClassLoader());
        final Transformation<RowData> gpuRuntimeInputTransform =
                createGpuRuntimeChain(inputTransform, gpuRuntimeConf, config, gpuRuntimeOutputRowType);

        final CodeGeneratorContext ctx =
                new CodeGeneratorContext(config, planner.getFlinkContext().getClassLoader())
                        .setOperatorBaseClass(getOperatorBaseClass());
        final CodeGenOperatorFactory<RowData> substituteStreamOperator =
                CalcCodeGenerator.generateCalcOperator(
                        ctx,
                        gpuRuntimeInputTransform,
                        (RowType) getOutputType(),
                        JavaScalaConversionUtil.toScala(rewrittenProjection),
                        JavaScalaConversionUtil.toScala(Optional.ofNullable(rewrittenCondition)),
                        isRetainHeader(),
                        getClass().getSimpleName());
        return ExecNodeUtil.createOneInputTransformation(
                gpuRuntimeInputTransform,
                createTransformationMeta(CALC_TRANSFORMATION, config),
                substituteStreamOperator,
                InternalTypeInfo.of(getOutputType()),
                gpuRuntimeInputTransform.getParallelism(),
                false);
    }

    private static String appendExternalRuntimeField(
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
