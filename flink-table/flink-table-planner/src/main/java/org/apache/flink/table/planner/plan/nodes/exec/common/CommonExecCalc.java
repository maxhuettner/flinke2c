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

package org.apache.flink.table.planner.plan.nodes.exec.common;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PhysicalTransformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.ScalarFunctionDefinition;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.codegen.CalcCodeGenerator;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.planner.functions.utils.ScalarSqlFunction;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.functions.table.externalruntime.ExternalRuntimePostOperator;
import org.apache.flink.table.runtime.functions.table.externalruntime.ExternalRuntimePreOperator;
import org.apache.flink.table.runtime.functions.table.externalruntime.RdmaPostOperator;
import org.apache.flink.table.runtime.functions.table.externalruntime.RdmaPreOperator;
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;

import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Map;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;


/** Base class for exec Calc. */
public abstract class CommonExecCalc extends ExecNodeBase<RowData>
        implements SingleTransformationTranslator<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(CommonExecCalc.class);

    public static final String CALC_TRANSFORMATION = "calc";

    public static final String CUSTOM_EXTERNAL_RUNTIME_FUNCTION_CLASS_NAME =
            "org.example.flinke2c.CurrencyConversionFunction";

    protected static final ConfigOption<String> EXTERNAL_RUNTIME_CONF_OPTION =
            ConfigOptions.key("table.exec.external-runtime.conf").stringType().noDefaultValue();
    public static final String EXTERNAL_RUNTIME_CONF_KEY_PREFIX = "table.exec.external-runtime.conf.";
    public static final String EXTERNAL_RUNTIME_CALC_PARALLELISM_PREFIX =
            "table.exec.external-runtime.calc-parallelism.";
    public static final ConfigOption<Integer> EXTERNAL_RUNTIME_CALC_PARALLELISM_OPTION =
            ConfigOptions.key("table.exec.external-runtime.calc-parallelism")
                    .intType()
                    .noDefaultValue();
    public static final ConfigOption<String> EXTERNAL_RUNTIME_FUNCTION_CLASS_OPTION =
            ConfigOptions.key("table.exec.external-runtime.function-class")
                    .stringType()
                    .noDefaultValue();
    public static final ConfigOption<Boolean> EXTERNAL_RUNTIME_CHAIN_ONLY_OPTION =
            ConfigOptions.key("table.exec.external-runtime.chain-only.enabled")
                    .booleanType()
                    .defaultValue(false);
    public static final String EXTERNAL_RUNTIME_FUNCTION_KIND_SCALAR = "scalar";
    public static final String EXTERNAL_RUNTIME_FUNCTION_KIND_FILTER = "filter";

    public static final String FIELD_NAME_PROJECTION = "projection";
    public static final String FIELD_NAME_CONDITION = "condition";

    @JsonProperty(FIELD_NAME_PROJECTION)
    protected final List<RexNode> projection;

    @JsonProperty(FIELD_NAME_CONDITION)
    protected final @Nullable RexNode condition;

    private final Class<?> operatorBaseClass;
    private final boolean retainHeader;

    protected CommonExecCalc(
            int id,
            ExecNodeContext context,
            ReadableConfig persistedConfig,
            List<RexNode> projection,
            @Nullable RexNode condition,
            Class<?> operatorBaseClass,
            boolean retainHeader,
            List<InputProperty> inputProperties,
            RowType outputType,
            String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        checkArgument(inputProperties.size() == 1);
        this.projection = checkNotNull(projection);
        this.condition = condition;
        this.operatorBaseClass = checkNotNull(operatorBaseClass);
        this.retainHeader = retainHeader;
    }

    protected Class<?> getOperatorBaseClass() {
        return operatorBaseClass;
    }

    protected boolean isRetainHeader() {
        return retainHeader;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "CommonExecCalc translateToPlanInternal: id={}, projectionSize={}, conditionPresent={}",
                    getId(),
                    projection.size(),
                    condition != null);
        }

        final RowType inputRowType = extractRowType(inputTransform);
        final RowType outputRowType = (RowType) getOutputType();
        final List<String> externalRuntimeFunctionClasses = resolveExternalRuntimeFunctionClasses(config);
        final ExternalRuntimeScalarFunctionRewriter rewriter =
                new ExternalRuntimeScalarFunctionRewriter(externalRuntimeFunctionClasses, inputRowType, outputRowType);
        final List<RexNode> rewrittenProjection = new ArrayList<>(projection.size());
        for (int i = 0; i < projection.size(); i++) {
            rewriter.setCurrentOutputFieldIndex(i);
            rewrittenProjection.add(projection.get(i).accept(rewriter));
        }
        rewriter.setCurrentOutputFieldIndex(-1);
        final @Nullable RexNode rewrittenCondition =
                condition == null ? null : condition.accept(rewriter);

        final boolean hasExternalRuntimeFunction = rewriter.hasExternalRuntimeFunction();
        final List<RexNode> effectiveProjection =
                hasExternalRuntimeFunction ? rewrittenProjection : projection;
        final @Nullable RexNode effectiveCondition =
                hasExternalRuntimeFunction ? rewrittenCondition : condition;
        final @Nullable String resolvedExternalRuntimeConf;
        if (hasExternalRuntimeFunction) {
            String externalRuntimeConf = rewriter.getExternalRuntimeConf();
            if (externalRuntimeConf.isEmpty()) {
                externalRuntimeConf = resolveExternalRuntimeConf(config, rewriter.getExternalRuntimeFunctionClass());
            }
            if (externalRuntimeConf.isEmpty()) {
                throw new TableException(
                        "External runtime function requires a TCP conf literal, "
                                + "table.exec.external-runtime.conf, "
                                + "or table.exec.external-runtime.conf.<functionClass>.");
            }
            externalRuntimeConf =
                    appendExternalRuntimeFunctionMetadata(
                            externalRuntimeConf,
                            rewriter.getExternalRuntimeFunctionClass(),
                            rewriter.getExternalRuntimeFunctionKind());
            externalRuntimeConf =
                    appendExternalRuntimeFunctionArgsMetadata(
                            externalRuntimeConf,
                            rewriter.getExternalRuntimeArgFieldIndices(),
                            rewriter.getExternalRuntimeArgFieldNames(),
                            rewriter.getExternalRuntimeArgFieldTypes());
            externalRuntimeConf =
                    appendExternalRuntimeFunctionResultMetadata(
                            externalRuntimeConf,
                            rewriter.getExternalRuntimeResultFieldIndices(),
                            rewriter.getExternalRuntimeResultFieldNames(),
                            rewriter.getExternalRuntimeResultFieldTypes(),
                            rewriter.getExternalRuntimeResultUdfFieldTypes(),
                            rewriter.getExternalRuntimeResultUdfFieldIndices());
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "External runtime rewrite injecting pre/post operators: functionClass={}, functionKind={}, resultFieldIndices={}",
                        rewriter.getExternalRuntimeFunctionClass(),
                        rewriter.getExternalRuntimeFunctionKind(),
                        rewriter.getExternalRuntimeResultFieldIndices());
            }
            resolvedExternalRuntimeConf = externalRuntimeConf;
        } else {
            resolvedExternalRuntimeConf = null;
        }

        final Transformation<RowData> externalRuntimeInputTransform;
        if (resolvedExternalRuntimeConf != null) {
            final RowType externalRuntimeOutputRowType =
                    applyResultTypes(
                            inputRowType,
                            rewriter.getExternalRuntimeResultFieldIndices(),
                            rewriter.getExternalRuntimeResultFieldTypes(),
                            planner.getFlinkContext().getClassLoader());
            externalRuntimeInputTransform =
                    createExternalRuntimeChain(
                            inputTransform, resolvedExternalRuntimeConf, config, externalRuntimeOutputRowType);
        } else {
            externalRuntimeInputTransform = inputTransform;
        }

        final CodeGeneratorContext ctx =
                new CodeGeneratorContext(config, planner.getFlinkContext().getClassLoader())
                        .setOperatorBaseClass(operatorBaseClass);

        final CodeGenOperatorFactory<RowData> substituteStreamOperator =
                CalcCodeGenerator.generateCalcOperator(
                        ctx,
                        externalRuntimeInputTransform,
                        (RowType) getOutputType(),
                        JavaScalaConversionUtil.toScala(effectiveProjection),
                        JavaScalaConversionUtil.toScala(Optional.ofNullable(effectiveCondition)),
                        retainHeader,
                        getClass().getSimpleName());
        return ExecNodeUtil.createOneInputTransformation(
                        externalRuntimeInputTransform,
                        createTransformationMeta(CALC_TRANSFORMATION, config),
                        substituteStreamOperator,
                        InternalTypeInfo.of(getOutputType()),
                        externalRuntimeInputTransform.getParallelism(),
                        false);
    }

    protected Transformation<RowData> createExternalRuntimeChain(
            Transformation<RowData> input,
            String conf,
            ExecNodeConfig config,
            @Nullable RowType postOutputRowType) {
        final RowType inputRowType = extractRowType(input);
        final RowType outputRowType = postOutputRowType == null ? inputRowType : postOutputRowType;
        final boolean rdma = isRdmaTransport(conf);
        LOG.info("External runtime transport selected: {} (conf={})", rdma ? "RDMA" : "TCP", conf);
        final OneInputTransformation<RowData, RowData> pre =
                ExecNodeUtil.createOneInputTransformation(
                        input,
                        createTransformationMeta(
                                "external-runtime-pre",
                                rdma ? "RdmaPre" : "ExternalRuntimePre",
                                rdma ? "RdmaPre" : "ExternalRuntimePre",
                                config),
                        rdma
                                ? new RdmaPreOperator(conf, inputRowType)
                                : new ExternalRuntimePreOperator(conf, inputRowType),
                        input.getOutputType(),
                        input.getParallelism(),
                        input.isParallelismConfigured());
        copyPlacementConstraints(input, pre);
        if (isChainOnly(config)) {
            pre.setChainingStrategy(ChainingStrategy.ALWAYS);
        }
        setMaxParallelismIfConfigured(input, pre);

        final OneInputTransformation<RowData, RowData> post =
                ExecNodeUtil.createOneInputTransformation(
                        pre,
                        createTransformationMeta(
                                "external-runtime-post",
                                rdma ? "RdmaPost" : "ExternalRuntimePost",
                                rdma ? "RdmaPost" : "ExternalRuntimePost",
                                config),
                        rdma
                                ? new RdmaPostOperator(conf, inputRowType, outputRowType)
                                : new ExternalRuntimePostOperator(conf, inputRowType, outputRowType),
                        InternalTypeInfo.of(outputRowType),
                        pre.getParallelism(),
                        pre.isParallelismConfigured());
        copyPlacementConstraints(input, post);
        if (isChainOnly(config)) {
            post.setChainingStrategy(ChainingStrategy.HEAD);
        }
        setMaxParallelismIfConfigured(input, post);

        return post;
    }

    private static boolean isRdmaTransport(@Nullable String conf) {
        if (conf == null) {
            return false;
        }
        for (String part : conf.split(";")) {
            int equals = part.indexOf('=');
            if (equals <= 0) {
                continue;
            }
            String key = part.substring(0, equals).trim();
            String value = part.substring(equals + 1).trim();
            if (("type".equalsIgnoreCase(key) || "transport".equalsIgnoreCase(key))
                    && "rdma".equalsIgnoreCase(value)) {
                return true;
            }
        }
        return false;
    }

    protected Transformation<RowData> createExternalRuntimeChain(
            Transformation<RowData> input, String conf, ExecNodeConfig config) {
        return createExternalRuntimeChain(input, conf, config, null);
    }

    protected static void copyPlacementConstraints(Transformation<?> from, Transformation<?> to) {
        from.getSlotSharingGroup().ifPresent(to::setSlotSharingGroup);
        if (from.getCoLocationGroupKey() != null) {
            to.setCoLocationGroupKey(from.getCoLocationGroupKey());
        }
    }

    protected static void setMaxParallelismIfConfigured(
            Transformation<?> from, Transformation<?> to) {
        if (from.getMaxParallelism() > 0) {
            to.setMaxParallelism(from.getMaxParallelism());
        }
    }


    @SuppressWarnings("unchecked")
    protected static RowType extractRowType(Transformation<RowData> input) {
        if (input.getOutputType() instanceof InternalTypeInfo) {
            return ((InternalTypeInfo<RowData>) input.getOutputType()).toRowType();
        }
        throw new TableException(
                "ExternalRuntimeOperator requires InternalTypeInfo output type, but was "
                        + input.getOutputType());
    }

    protected static RowType applyResultTypes(
            RowType inputRowType,
            @Nullable List<Integer> resultFieldIndices,
            @Nullable List<String> resultFieldTypes,
            ClassLoader classLoader) {
        if (resultFieldIndices == null
                || resultFieldIndices.isEmpty()
                || resultFieldTypes == null
                || resultFieldTypes.isEmpty()) {
            return inputRowType;
        }
        final List<RowType.RowField> fields = inputRowType.getFields();
        final int fieldCount = fields.size();
        final LogicalType[] types = new LogicalType[fieldCount];
        final String[] names = new String[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            final RowType.RowField field = fields.get(i);
            types[i] = field.getType();
            names[i] = field.getName();
        }
        final int max = Math.min(resultFieldIndices.size(), resultFieldTypes.size());
        for (int i = 0; i < max; i++) {
            final Integer idx = resultFieldIndices.get(i);
            final String typeString = resultFieldTypes.get(i);
            if (idx == null
                    || idx < 0
                    || idx >= fieldCount
                    || typeString == null
                    || typeString.trim().isEmpty()) {
                continue;
            }
            types[idx] = LogicalTypeParser.parse(typeString, classLoader);
        }
        return RowType.of(types, names);
    }

    protected static String appendExternalRuntimeFunctionMetadata(
            String conf, @Nullable String functionClass, @Nullable String functionKind) {
        String result = conf;
        if (functionClass != null
                && !functionClass.isEmpty()
                && !containsConfKey(result, "class")
                && !containsConfKey(result, "functionclass")) {
            result = appendConfValue(result, "class", functionClass);
        }
        if (functionKind != null
                && !functionKind.isEmpty()
                && !containsConfKey(result, "type")
                && !containsConfKey(result, "functionkind")) {
            result = appendConfValue(result, "type", functionKind);
        }
        return result;
    }

    protected static String appendExternalRuntimeFunctionArgsMetadata(
            String conf,
            @Nullable List<Integer> argFieldIndices,
            @Nullable List<String> argFieldNames,
            @Nullable List<String> argFieldTypes) {
        String result = conf;
        if (argFieldIndices != null
                && !argFieldIndices.isEmpty()
                && !containsConfKey(result, "argfieldindices")) {
            result =
                    appendConfValue(
                            result,
                            "argFieldIndices",
                            joinIntList(argFieldIndices));
        }
        if (argFieldNames != null
                && !argFieldNames.isEmpty()
                && !containsConfKey(result, "argfieldnames")) {
            result =
                    appendConfValue(
                            result,
                            "argFieldNames",
                            joinStringList(argFieldNames));
        }
        if (argFieldTypes != null
                && !argFieldTypes.isEmpty()
                && !containsConfKey(result, "argfieldtypes")) {
            result =
                    appendConfValue(
                            result,
                            "argFieldTypes",
                            joinStringList(argFieldTypes));
        }
        return result;
    }

    protected static String appendExternalRuntimeFunctionResultMetadata(
            String conf,
            @Nullable List<Integer> resultFieldIndices,
            @Nullable List<String> resultFieldNames,
            @Nullable List<String> resultFieldTypes,
            @Nullable List<String> resultUdfFieldTypes,
            @Nullable List<Integer> resultUdfFieldIndices) {
        String result = conf;
        if (resultFieldIndices != null
                && !resultFieldIndices.isEmpty()
                && !containsConfKey(result, "resultfieldindices")) {
            result =
                    appendConfValue(
                            result,
                            "resultFieldIndices",
                            joinIntList(resultFieldIndices));
        }
        if (resultFieldNames != null
                && !resultFieldNames.isEmpty()
                && !containsConfKey(result, "resultfieldnames")) {
            result =
                    appendConfValue(
                            result,
                            "resultFieldNames",
                            joinStringList(resultFieldNames));
        }
        if (resultFieldTypes != null
                && !resultFieldTypes.isEmpty()
                && !containsConfKey(result, "resultfieldtypes")) {
            result =
                    appendConfValue(
                            result,
                            "resultFieldTypes",
                            joinStringList(resultFieldTypes));
        }
        if (resultUdfFieldTypes != null
                && !resultUdfFieldTypes.isEmpty()
                && !containsConfKey(result, "resultudffieldtypes")) {
            result =
                    appendConfValue(
                            result,
                            "resultUdfFieldTypes",
                            joinStringList(resultUdfFieldTypes));
        }
        if (resultUdfFieldIndices != null
                && !resultUdfFieldIndices.isEmpty()
                && !containsConfKey(result, "resultudffieldindices")) {
            result =
                    appendConfValue(
                            result,
                            "resultUdfFieldIndices",
                            joinIntList(resultUdfFieldIndices));
        }
        return result;
    }

    protected static boolean containsConfKey(String conf, String keyLower) {
        if (conf == null || conf.isEmpty()) {
            return false;
        }
        final String[] parts = conf.split(";");
        for (String part : parts) {
            final String trimmed = part.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            final int idx = trimmed.indexOf('=');
            if (idx <= 0) {
                continue;
            }
            final String k = trimmed.substring(0, idx).trim().toLowerCase();
            if (k.equals(keyLower)) {
                return true;
            }
        }
        return false;
    }

    protected static String appendConfValue(String conf, String key, String value) {
        if (conf == null || conf.isEmpty()) {
            return key + "=" + value;
        }
        if (conf.endsWith(";")) {
            return conf + key + "=" + value;
        }
        return conf + ";" + key + "=" + value;
    }

    public static List<String> parseExternalRuntimeFunctionClasses(
            @Nullable String value, String defaultClass) {
        final List<String> classes = new ArrayList<>();
        if (value != null && !value.trim().isEmpty()) {
            final String[] parts = value.split(",");
            for (String part : parts) {
                final String trimmed = part.trim();
                if (!trimmed.isEmpty()) {
                    classes.add(trimmed);
                }
            }
        }
        if (classes.isEmpty() && defaultClass != null && !defaultClass.isEmpty()) {
            classes.add(defaultClass);
        }
        return classes;
    }

    public static @Nullable String resolveFunctionClassConfig(ReadableConfig config) {
        return config.getOptional(EXTERNAL_RUNTIME_FUNCTION_CLASS_OPTION).orElse(null);
    }

    public static List<String> resolveExternalRuntimeFunctionClasses(ReadableConfig config) {
        return parseExternalRuntimeFunctionClasses(
                resolveFunctionClassConfig(config), CUSTOM_EXTERNAL_RUNTIME_FUNCTION_CLASS_NAME);
    }

    public static @Nullable Integer resolveExternalRuntimeCalcParallelism(
            ExecNodeConfig config, List<RexNode> projection, @Nullable RexNode condition) {
        final Integer global = config.getOptional(EXTERNAL_RUNTIME_CALC_PARALLELISM_OPTION).orElse(null);
        if (global != null) {
            return global;
        }
        final Map<String, String> map = config.toMap();
        if (map.isEmpty()) {
            return null;
        }
        final Map<String, Integer> configured = new java.util.LinkedHashMap<>();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            final String key = entry.getKey();
            if (!key.startsWith(EXTERNAL_RUNTIME_CALC_PARALLELISM_PREFIX)) {
                continue;
            }
            final String suffix = key.substring(EXTERNAL_RUNTIME_CALC_PARALLELISM_PREFIX.length());
            if (suffix.isEmpty()) {
                continue;
            }
            final String value = entry.getValue();
            if (value == null || value.trim().isEmpty()) {
                continue;
            }
            try {
                configured.put(suffix, Integer.parseInt(value.trim()));
            } catch (NumberFormatException e) {
                throw new TableException(
                        "Invalid external runtime calc parallelism for " + suffix + ": " + value,
                        e);
            }
        }
        if (configured.isEmpty()) {
            return null;
        }
        final String matched =
                findExternalRuntimeFunctionClass(new ArrayList<>(configured.keySet()), projection, condition);
        if (matched != null) {
            return configured.get(matched);
        }
        if (configured.size() == 1) {
            final Map.Entry<String, Integer> entry = configured.entrySet().iterator().next();
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "External runtime calc parallelism fallback: no match found, applying {}={}",
                        entry.getKey(),
                        entry.getValue());
            }
            return entry.getValue();
        }
        return null;
    }

    public static String resolveExternalRuntimeConf(ReadableConfig config, @Nullable String functionClass) {
        return resolveExternalRuntimeConfWithPrefix(
                config, functionClass, EXTERNAL_RUNTIME_CONF_KEY_PREFIX, EXTERNAL_RUNTIME_CONF_OPTION);
    }

    private static String resolveExternalRuntimeConfWithPrefix(
            ReadableConfig config,
            @Nullable String functionClass,
            String prefix,
            ConfigOption<String> fallback) {
        if (functionClass != null && !functionClass.isEmpty()) {
            final String directKey = prefix + functionClass;
            final String direct =
                    config.getOptional(ConfigOptions.key(directKey).stringType().noDefaultValue())
                            .orElse("");
            if (!direct.isEmpty()) {
                return direct;
            }
            final String simple = deriveSimpleName(functionClass);
            if (simple != null && !simple.isEmpty()) {
                final String simpleKey = prefix + simple;
                final String simpleValue =
                        config.getOptional(
                                        ConfigOptions.key(simpleKey)
                                                .stringType()
                                                .noDefaultValue())
                                .orElse("");
                if (!simpleValue.isEmpty()) {
                    return simpleValue;
                }
            }
        }
        return config.getOptional(fallback).orElse("");
    }

    private static String deriveSimpleName(String className) {
        if (className == null) {
            return null;
        }
        final int lastDot = className.lastIndexOf('.');
        String simple = lastDot < 0 ? className : className.substring(lastDot + 1);
        final int suffixIndex = simple.indexOf('$');
        if (suffixIndex > 0) {
            simple = simple.substring(0, suffixIndex);
        }
        return simple;
    }

    private static boolean isChainOnly(ReadableConfig config) {
        return Boolean.TRUE.equals(config.get(EXTERNAL_RUNTIME_CHAIN_ONLY_OPTION));
    }

    private static String joinIntList(List<Integer> values) {
        final StringBuilder sb = new StringBuilder(values.size() * 4);
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(values.get(i));
        }
        return sb.toString();
    }

    private static String joinStringList(List<String> values) {
        final StringBuilder sb = new StringBuilder(values.size() * 8);
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(encodeConfComponent(values.get(i)));
        }
        return sb.toString();
    }

    private static String encodeConfComponent(@Nullable String value) {
        if (value == null) {
            return "";
        }
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private static boolean matchesOperatorName(
            @Nullable String name, @Nullable String targetSimpleName) {
        if (name == null) {
            return false;
        }
        final String trimmed = name.trim();
        if (trimmed.isEmpty()) {
            return false;
        }
        final String normalized = trimmed.replace("\"", "").replace("`", "");
        if (targetSimpleName != null && normalized.equalsIgnoreCase(targetSimpleName)) {
            return true;
        }
        final int lastDot = normalized.lastIndexOf('.');
        if (lastDot >= 0 && lastDot < normalized.length() - 1) {
            final String simple = normalized.substring(lastDot + 1);
            if (targetSimpleName != null && simple.equalsIgnoreCase(targetSimpleName)) {
                return true;
            }
        }
        final int suffixIndex = normalized.indexOf('$');
        if (suffixIndex > 0) {
            return targetSimpleName != null
                    && normalized.substring(0, suffixIndex).equalsIgnoreCase(targetSimpleName);
        }
        return false;
    }

    private static boolean isExternalRuntimeScalarFunction(
            RexCall call, String targetClassName, @Nullable String targetSimpleName) {
        final SqlOperator operator = call.getOperator();
        if (matchesOperatorName(operator.getName(), targetSimpleName)) {
            return true;
        }
        if (operator instanceof ScalarSqlFunction) {
            final ScalarSqlFunction function = (ScalarSqlFunction) operator;
            return classNameEquals(
                    targetClassName, function.scalarFunction().getClass().getName());
        }
        if (operator instanceof BridgingSqlFunction) {
            final BridgingSqlFunction bridging = (BridgingSqlFunction) operator;
            final String identifierName =
                    bridging.getResolvedFunction()
                            .getIdentifier()
                            .map(FunctionIdentifier::getFunctionName)
                            .orElse(null);
            if (matchesOperatorName(identifierName, targetSimpleName)) {
                return true;
            }
            final FunctionDefinition definition = bridging.getDefinition();
            return matchesDefinition(definition, targetClassName);
        }
        return false;
    }

    private static boolean matchesDefinition(
            @Nullable FunctionDefinition definition, String targetClassName) {
        if (definition == null) {
            return false;
        }
        if (definition instanceof ScalarFunctionDefinition) {
            final ScalarFunction scalarFunction =
                    ((ScalarFunctionDefinition) definition).getScalarFunction();
            return classNameEquals(targetClassName, scalarFunction.getClass().getName());
        }
        if (definition instanceof ScalarFunction) {
            return classNameEquals(targetClassName, definition.getClass().getName());
        }
        return false;
    }

    private static boolean classNameEquals(String expected, String actual) {
        if (expected == null || actual == null) {
            return false;
        }
        return expected.equals(actual) || expected.equalsIgnoreCase(actual);
    }

    private static boolean isSupportedExternalRuntimeOperand(RexNode operand) {
        if (operand instanceof RexInputRef || operand instanceof RexFieldAccess) {
            return true;
        }
        if (operand instanceof RexCall) {
            final RexCall call = (RexCall) operand;
            if (call.getKind() == SqlKind.CAST || call.getKind() == SqlKind.AS) {
                return isSupportedExternalRuntimeOperand(call.getOperands().get(0));
            }
        }
        return false;
    }

    private static String mergeExternalRuntimeConf(@Nullable String existing, String conf) {
        if (conf == null || conf.isEmpty()) {
            return existing == null ? "" : existing;
        }
        if (existing == null || existing.isEmpty()) {
            return conf;
        }
        if (!existing.equals(conf)) {
            throw new TableException(
                    "External runtime function requires a single, consistent conf literal.");
        }
        return existing;
    }

    private static String extractExternalRuntimeConf(List<RexNode> operands) {
        for (RexNode operand : operands) {
            if (operand instanceof RexLiteral) {
                final String conf = RexLiteral.stringValue((RexLiteral) operand);
                if (conf != null) {
                    return conf;
                }
            }
            if (operand.getKind() == SqlKind.DEFAULT) {
                return "";
            }
        }
        return "";
    }

    public static final class ExternalRuntimeScalarFunctionRewriter extends RexShuttle {
        private final List<TargetFunction> targets;
        private final RowType inputRowType;
        private final RowType outputRowType;
        private boolean externalRuntimeFunctionFound;
        private String externalRuntimeFunctionKind = EXTERNAL_RUNTIME_FUNCTION_KIND_SCALAR;
        private @Nullable String matchedFunctionClass;
        private @Nullable String externalRuntimeConf;
        private int currentOutputFieldIndex = -1;
        private @Nullable Integer currentUdfFieldIndexOverride;
        private final List<Integer> externalRuntimeArgFieldIndices = new ArrayList<>();
        private final List<String> externalRuntimeArgFieldNames = new ArrayList<>();
        private final List<String> externalRuntimeArgFieldTypes = new ArrayList<>();
        private final List<Integer> externalRuntimeResultFieldIndices = new ArrayList<>();
        private final List<String> externalRuntimeResultFieldNames = new ArrayList<>();
        private final List<String> externalRuntimeResultFieldTypes = new ArrayList<>();
        private final List<Integer> externalRuntimeResultUdfFieldIndices = new ArrayList<>();
        private final List<String> externalRuntimeResultUdfFieldTypes = new ArrayList<>();

        public ExternalRuntimeScalarFunctionRewriter(
                List<String> targetClassNames, RowType inputRowType, RowType outputRowType) {
            this.targets = buildTargets(targetClassNames);
            this.inputRowType = checkNotNull(inputRowType, "inputRowType");
            this.outputRowType = checkNotNull(outputRowType, "outputRowType");
        }

        public void setCurrentOutputFieldIndex(int outputFieldIndex) {
            this.currentOutputFieldIndex = outputFieldIndex;
        }

        @Override
        public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
            final RexCall externalRuntimeCall = unwrapExternalRuntimeCall(fieldAccess.getReferenceExpr());
            if (externalRuntimeCall == null || fieldAccess.getField() == null) {
                return super.visitFieldAccess(fieldAccess);
            }
            final int udfFieldIndex = fieldAccess.getField().getIndex();
            currentUdfFieldIndexOverride = udfFieldIndex;
            try {
                // Drop the field access; the external runtime uses the recorded udfFieldIndex.
                return externalRuntimeCall.accept(this);
            } finally {
                currentUdfFieldIndexOverride = null;
            }
        }

        @Override
        public RexNode visitCall(RexCall call) {
            final TargetFunction match = matchTarget(call);
            if (match == null) {
                return super.visitCall(call);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("External runtime rewrite matched UDF: {}", match.className);
            }
            if (matchedFunctionClass == null) {
                matchedFunctionClass = match.className;
            } else if (!matchedFunctionClass.equals(match.className)) {
                throw new TableException(
                        "External runtime function supports a single function class per Calc.");
            }
            final List<RexNode> operands = call.getOperands();
            if (operands.isEmpty()) {
                throw new TableException("External runtime function requires at least one argument.");
            }
            externalRuntimeFunctionFound = true;
            externalRuntimeConf = mergeExternalRuntimeConf(externalRuntimeConf, extractExternalRuntimeConf(operands));
            if (currentOutputFieldIndex < 0) {
                externalRuntimeFunctionKind = EXTERNAL_RUNTIME_FUNCTION_KIND_FILTER;
            }
            boolean foundFieldArg = false;
            RexNode firstFieldOperand = null;
            @Nullable Integer firstFieldIndex = null;
            for (RexNode operand : operands) {
                if (operand instanceof RexLiteral || operand.getKind() == SqlKind.DEFAULT) {
                    continue;
                }
                if (!isSupportedExternalRuntimeOperand(operand)) {
                    throw new TableException(
                            "External runtime function requires column references for all non-literal arguments.");
                }
                final @Nullable Integer fieldIndex = extractExternalRuntimeFieldIndex(operand);
                if (fieldIndex == null) {
                    throw new TableException(
                            "External runtime function requires input references as arguments.");
                }
                addArgField(operand, fieldIndex);
                if (firstFieldOperand == null) {
                    firstFieldOperand = operand;
                    firstFieldIndex = fieldIndex;
                }
                foundFieldArg = true;
            }
            if (!foundFieldArg) {
                throw new TableException(
                        "External runtime function requires at least one column reference argument.");
            }
            if (currentOutputFieldIndex < 0) {
                return call;
            }
            final String udfReturnType = resolveUdfReturnType(call, currentUdfFieldIndexOverride);
            addResultField(currentUdfFieldIndexOverride, firstFieldIndex, udfReturnType);
            return firstFieldOperand.accept(this);
        }

        public boolean hasExternalRuntimeFunction() {
            return externalRuntimeFunctionFound;
        }

        public String getExternalRuntimeConf() {
            return externalRuntimeConf == null ? "" : externalRuntimeConf;
        }

        public String getExternalRuntimeFunctionClass() {
            if (matchedFunctionClass != null) {
                return matchedFunctionClass;
            }
            return targets.isEmpty() ? "" : targets.get(0).className;
        }

        public String getExternalRuntimeFunctionKind() {
            return externalRuntimeFunctionKind;
        }

        public List<Integer> getExternalRuntimeArgFieldIndices() {
            return externalRuntimeArgFieldIndices;
        }

        public List<String> getExternalRuntimeArgFieldNames() {
            return externalRuntimeArgFieldNames;
        }

        public List<String> getExternalRuntimeArgFieldTypes() {
            return externalRuntimeArgFieldTypes;
        }

        public List<Integer> getExternalRuntimeResultFieldIndices() {
            return externalRuntimeResultFieldIndices;
        }

        public List<String> getExternalRuntimeResultFieldNames() {
            return externalRuntimeResultFieldNames;
        }

        public List<String> getExternalRuntimeResultFieldTypes() {
            return externalRuntimeResultFieldTypes;
        }

        public List<Integer> getExternalRuntimeResultUdfFieldIndices() {
            return externalRuntimeResultUdfFieldIndices;
        }

        public List<String> getExternalRuntimeResultUdfFieldTypes() {
            return externalRuntimeResultUdfFieldTypes;
        }

        private void addArgField(int fieldIndex) {
            externalRuntimeArgFieldIndices.add(fieldIndex);
            externalRuntimeArgFieldNames.add(resolveFieldName(inputRowType, fieldIndex));
            externalRuntimeArgFieldTypes.add(resolveFieldType(inputRowType, fieldIndex));
        }

        private void addResultField(
                @Nullable Integer udfFieldIndex,
                @Nullable Integer targetFieldIndex,
                @Nullable String udfFieldType) {
            if (currentOutputFieldIndex < 0) {
                return;
            }
            if (targetFieldIndex == null || targetFieldIndex < 0) {
                return;
            }
            if (externalRuntimeResultFieldIndices.contains(targetFieldIndex)) {
                return;
            }
            externalRuntimeResultFieldIndices.add(targetFieldIndex);
            externalRuntimeResultFieldNames.add(resolveFieldName(outputRowType, currentOutputFieldIndex));
            externalRuntimeResultFieldTypes.add(resolveFieldType(outputRowType, currentOutputFieldIndex));
            externalRuntimeResultUdfFieldIndices.add(udfFieldIndex == null ? -1 : udfFieldIndex);
            externalRuntimeResultUdfFieldTypes.add(udfFieldType);
        }

        private @Nullable String resolveUdfReturnType(
                RexCall call, @Nullable Integer udfFieldIndex) {
            if (call == null) {
                return null;
            }
            final RelDataType relType = call.getType();
            if (udfFieldIndex != null && udfFieldIndex >= 0 && relType != null) {
                final List<RelDataTypeField> fields = relType.getFieldList();
                if (fields != null && udfFieldIndex < fields.size()) {
                    return FlinkTypeFactory.toLogicalType(fields.get(udfFieldIndex).getType())
                            .asSerializableString();
                }
            }
            return relType == null
                    ? null
                    : FlinkTypeFactory.toLogicalType(relType).asSerializableString();
        }

        private @Nullable RexCall unwrapExternalRuntimeCall(RexNode node) {
            if (!(node instanceof RexCall)) {
                return null;
            }
            final RexCall call = (RexCall) node;
            if (matchTarget(call) != null) {
                return call;
            }
            if (call.getKind() == SqlKind.CAST || call.getKind() == SqlKind.AS) {
                return unwrapExternalRuntimeCall(call.getOperands().get(0));
            }
            return null;
        }

        private void addArgField(RexNode operand, int fieldIndex) {
            externalRuntimeArgFieldIndices.add(fieldIndex);
            externalRuntimeArgFieldNames.add(resolveFieldName(inputRowType, fieldIndex));
            externalRuntimeArgFieldTypes.add(resolveOperandType(inputRowType, operand, fieldIndex));
        }

        private TargetFunction matchTarget(RexCall call) {
            for (TargetFunction target : targets) {
                if (isExternalRuntimeScalarFunction(call, target.className, target.simpleName)) {
                    return target;
                }
            }
            return null;
        }
    }

    private static List<TargetFunction> buildTargets(List<String> classNames) {
        final List<TargetFunction> targets = new ArrayList<>();
        if (classNames != null) {
            for (String className : classNames) {
                if (className == null || className.trim().isEmpty()) {
                    continue;
                }
                targets.add(new TargetFunction(className.trim(), deriveSimpleName(className)));
            }
        }
        return targets;
    }

    private static final class TargetFunction {
        private final String className;
        private final String simpleName;

        private TargetFunction(String className, String simpleName) {
            this.className = className;
            this.simpleName = simpleName;
        }
    }

    private static @Nullable String findExternalRuntimeFunctionClass(
            List<String> classNames, List<RexNode> projection, @Nullable RexNode condition) {
        if (classNames == null || classNames.isEmpty()) {
            return null;
        }
        final ExternalRuntimeFunctionDetector detector =
                new ExternalRuntimeFunctionDetector(buildTargets(classNames));
        for (RexNode node : projection) {
            detector.scan(node);
        }
        if (condition != null) {
            detector.scan(condition);
        }
        return detector.getMatchedClass();
    }

    private static final class ExternalRuntimeFunctionDetector {
        private final List<TargetFunction> targets;
        private @Nullable String matchedClass;

        private ExternalRuntimeFunctionDetector(List<TargetFunction> targets) {
            this.targets = targets;
        }

        private void scan(@Nullable RexNode node) {
            if (node == null) {
                return;
            }
            if (node instanceof RexCall) {
                final RexCall call = (RexCall) node;
                for (TargetFunction target : targets) {
                    if (isExternalRuntimeScalarFunction(call, target.className, target.simpleName)) {
                        if (matchedClass == null) {
                            matchedClass = target.className;
                        } else if (!matchedClass.equals(target.className)) {
                            throw new TableException(
                                    "External runtime calc parallelism requires a single function class per Calc.");
                        }
                        break;
                    }
                }
                for (RexNode operand : call.getOperands()) {
                    scan(operand);
                }
                return;
            }
            if (node instanceof RexFieldAccess) {
                scan(((RexFieldAccess) node).getReferenceExpr());
            }
        }

        private @Nullable String getMatchedClass() {
            return matchedClass;
        }
    }

    private static @Nullable Integer extractExternalRuntimeFieldIndex(RexNode operand) {
        if (operand instanceof RexInputRef) {
            return ((RexInputRef) operand).getIndex();
        }
        if (operand instanceof RexFieldAccess) {
            final RexNode ref = ((RexFieldAccess) operand).getReferenceExpr();
            if (ref instanceof RexInputRef) {
                return ((RexInputRef) ref).getIndex();
            }
            if (ref instanceof RexCall) {
                final RexCall call = (RexCall) ref;
                if (call.getKind() == SqlKind.CAST || call.getKind() == SqlKind.AS) {
                    return extractExternalRuntimeFieldIndex(call.getOperands().get(0));
                }
            }
            return null;
        }
        if (operand instanceof RexCall) {
            final RexCall call = (RexCall) operand;
            if (call.getKind() == SqlKind.CAST || call.getKind() == SqlKind.AS) {
                return extractExternalRuntimeFieldIndex(call.getOperands().get(0));
            }
        }
        return null;
    }

    private static String resolveFieldName(RowType rowType, int fieldIndex) {
        final List<RowType.RowField> fields = rowType.getFields();
        if (fieldIndex < 0 || fieldIndex >= fields.size()) {
            throw new TableException(
                    "External runtime function argument index out of bounds: " + fieldIndex);
        }
        return fields.get(fieldIndex).getName();
    }

    private static String resolveFieldType(RowType rowType, int fieldIndex) {
        final List<RowType.RowField> fields = rowType.getFields();
        if (fieldIndex < 0 || fieldIndex >= fields.size()) {
            throw new TableException(
                    "External runtime function argument index out of bounds: " + fieldIndex);
        }
        return fields.get(fieldIndex).getType().asSerializableString();
    }

    private static String resolveOperandType(RowType rowType, RexNode operand, int fieldIndex) {
        if (operand instanceof RexCall) {
            final RexCall call = (RexCall) operand;
            if (call.getKind() == SqlKind.CAST || call.getKind() == SqlKind.AS) {
                return FlinkTypeFactory.toLogicalType(call.getType()).asSerializableString();
            }
        }
        if (operand instanceof RexFieldAccess) {
            final RexNode ref = ((RexFieldAccess) operand).getReferenceExpr();
            if (ref instanceof RexCall) {
                final RexCall call = (RexCall) ref;
                if (call.getKind() == SqlKind.CAST || call.getKind() == SqlKind.AS) {
                    return FlinkTypeFactory.toLogicalType(call.getType()).asSerializableString();
                }
            }
        }
        return resolveFieldType(rowType, fieldIndex);
    }

}
