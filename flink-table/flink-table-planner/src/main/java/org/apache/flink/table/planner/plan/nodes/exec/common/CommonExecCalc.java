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
import org.apache.flink.table.runtime.functions.table.ProxyOperator;
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
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;


/** Base class for exec Calc. */
public abstract class CommonExecCalc extends ExecNodeBase<RowData>
        implements SingleTransformationTranslator<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(CommonExecCalc.class);

    public static final String CALC_TRANSFORMATION = "calc";

    public static final String CUSTOM_PROXY_FUNCTION_NAME =
            "CurrencyConversionFunction";

    public static final String CUSTOM_PROXY_FUNCTION_CLASS_NAME =
            "org.example.flinke2c.CurrencyConversionFunction";

    protected static final ConfigOption<String> PROXY_CONF_OPTION =
            ConfigOptions.key("table.exec.proxy.conf").stringType().noDefaultValue();
    public static final ConfigOption<String> PROXY_FUNCTION_CLASS_OPTION =
            ConfigOptions.key("table.exec.proxy.function-class")
                    .stringType()
                    .defaultValue(CUSTOM_PROXY_FUNCTION_CLASS_NAME);
    public static final ConfigOption<Boolean> PROXY_CHAIN_ONLY_OPTION =
            ConfigOptions.key("table.exec.proxy.chain-only.enabled")
                    .booleanType()
                    .defaultValue(false);
    public static final String PROXY_FUNCTION_KIND_SCALAR = "scalar";

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
        LOG.info(
                "CommonExecCalc translateToPlanInternal: id={}, projectionSize={}, conditionPresent={}",
                getId(),
                projection.size(),
                condition != null);

        final RowType inputRowType = extractRowType(inputTransform);
        final RowType outputRowType = (RowType) getOutputType();
        final String proxyFunctionClass =
                config.getOptional(PROXY_FUNCTION_CLASS_OPTION)
                        .orElse(CUSTOM_PROXY_FUNCTION_CLASS_NAME);
        final ProxyScalarFunctionRewriter rewriter =
                new ProxyScalarFunctionRewriter(proxyFunctionClass, inputRowType, outputRowType);
        final List<RexNode> rewrittenProjection = new ArrayList<>(projection.size());
        for (int i = 0; i < projection.size(); i++) {
            rewriter.setCurrentOutputFieldIndex(i);
            rewrittenProjection.add(projection.get(i).accept(rewriter));
        }
        rewriter.setCurrentOutputFieldIndex(-1);
        final @Nullable RexNode rewrittenCondition =
                condition == null ? null : condition.accept(rewriter);

        final boolean hasProxyFunction = rewriter.hasProxyFunction();
        final List<RexNode> effectiveProjection =
                hasProxyFunction ? rewrittenProjection : projection;
        final @Nullable RexNode effectiveCondition =
                hasProxyFunction ? rewrittenCondition : condition;
        final @Nullable String resolvedProxyConf;
        if (hasProxyFunction) {
            String proxyConf = rewriter.getProxyConf();
            if (proxyConf.isEmpty()) {
                proxyConf = config.getOptional(PROXY_CONF_OPTION).orElse("");
            }
            if (proxyConf.isEmpty()) {
                throw new TableException(
                        "Proxy scalar function requires a TCP conf literal or table.exec.proxy.conf.");
            }
            proxyConf =
                    appendProxyFunctionMetadata(
                            proxyConf,
                            rewriter.getProxyFunctionClass(),
                            rewriter.getProxyFunctionKind());
            proxyConf =
                    appendProxyFunctionArgsMetadata(
                            proxyConf,
                            rewriter.getProxyArgFieldIndices(),
                            rewriter.getProxyArgFieldNames(),
                            rewriter.getProxyArgFieldTypes());
            proxyConf =
                    appendProxyFunctionResultMetadata(
                            proxyConf,
                            rewriter.getProxyResultFieldIndices(),
                            rewriter.getProxyResultFieldNames(),
                            rewriter.getProxyResultFieldTypes(),
                            rewriter.getProxyResultUdfFieldTypes(),
                            rewriter.getProxyResultUdfFieldIndices());
            LOG.info(
                    "Proxy rewrite injecting pre/post operators for scalar UDF: functionClass={}, functionKind={}, resultFieldIndices={}, conf={}",
                    rewriter.getProxyFunctionClass(),
                    rewriter.getProxyFunctionKind(),
                    rewriter.getProxyResultFieldIndices(),
                    proxyConf);
            resolvedProxyConf = proxyConf;
        } else {
            resolvedProxyConf = null;
        }

        final Transformation<RowData> proxyInputTransform;
        if (resolvedProxyConf != null) {
            final RowType proxyOutputRowType =
                    applyResultTypes(
                            inputRowType,
                            rewriter.getProxyResultFieldIndices(),
                            rewriter.getProxyResultFieldTypes(),
                            planner.getFlinkContext().getClassLoader());
            proxyInputTransform =
                    createProxyChain(inputTransform, resolvedProxyConf, config, proxyOutputRowType);
        } else {
            proxyInputTransform = inputTransform;
        }

        final CodeGeneratorContext ctx =
                new CodeGeneratorContext(config, planner.getFlinkContext().getClassLoader())
                        .setOperatorBaseClass(operatorBaseClass);

        final CodeGenOperatorFactory<RowData> substituteStreamOperator =
                CalcCodeGenerator.generateCalcOperator(
                        ctx,
                        proxyInputTransform,
                        (RowType) getOutputType(),
                        JavaScalaConversionUtil.toScala(effectiveProjection),
                        JavaScalaConversionUtil.toScala(Optional.ofNullable(effectiveCondition)),
                        retainHeader,
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

    protected Transformation<RowData> createProxyChain(
            Transformation<RowData> input,
            String conf,
            ExecNodeConfig config,
            @Nullable RowType postOutputRowType) {
        final RowType inputRowType = extractRowType(input);
        final RowType outputRowType = postOutputRowType == null ? inputRowType : postOutputRowType;
        final OneInputTransformation<RowData, RowData> pre =
                ExecNodeUtil.createOneInputTransformation(
                        input,
                        createTransformationMeta("proxy-pre", "ProxyPre", "ProxyPre", config),
                        new ProxyOperator(conf, ProxyOperator.Side.PRE, inputRowType, inputRowType),
                        input.getOutputType(),
                        input.getParallelism(),
                        input.isParallelismConfigured());
        copyPlacementConstraints(input, pre);
        if (Boolean.TRUE.equals(config.get(PROXY_CHAIN_ONLY_OPTION))) {
            pre.setChainingStrategy(ChainingStrategy.ALWAYS);
        }
        setMaxParallelismIfConfigured(input, pre);

        final OneInputTransformation<RowData, RowData> post =
                ExecNodeUtil.createOneInputTransformation(
                        pre,
                        createTransformationMeta("proxy-post", "ProxyPost", "ProxyPost", config),
                        new ProxyOperator(conf, ProxyOperator.Side.POST, inputRowType, outputRowType),
                        InternalTypeInfo.of(outputRowType),
                        pre.getParallelism(),
                        pre.isParallelismConfigured());
        copyPlacementConstraints(input, post);
        if (Boolean.TRUE.equals(config.get(PROXY_CHAIN_ONLY_OPTION))) {
            post.setChainingStrategy(ChainingStrategy.HEAD);
        }
        setMaxParallelismIfConfigured(input, post);

        return post;
    }

    protected Transformation<RowData> createProxyChain(
            Transformation<RowData> input, String conf, ExecNodeConfig config) {
        return createProxyChain(input, conf, config, null);
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
                "ProxyOperator requires InternalTypeInfo output type, but was "
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

    protected static String appendProxyFunctionMetadata(
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

    protected static String appendProxyFunctionArgsMetadata(
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

    protected static String appendProxyFunctionResultMetadata(
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

    private static boolean isProxyScalarFunction(
            RexCall call, String targetClassName, @Nullable String targetSimpleName) {
        final SqlOperator operator = call.getOperator();
        if (matchesOperatorName(operator.getName(), targetSimpleName)) {
            return true;
        }
        if (operator instanceof ScalarSqlFunction) {
            final ScalarSqlFunction function = (ScalarSqlFunction) operator;
            return targetClassName.equals(function.scalarFunction().getClass().getName());
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
            return targetClassName.equals(scalarFunction.getClass().getName());
        }
        if (definition instanceof ScalarFunction) {
            return targetClassName.equals(definition.getClass().getName());
        }
        return false;
    }

    private static boolean isSupportedProxyOperand(RexNode operand) {
        if (operand instanceof RexInputRef || operand instanceof RexFieldAccess) {
            return true;
        }
        if (operand instanceof RexCall) {
            final RexCall call = (RexCall) operand;
            if (call.getKind() == SqlKind.CAST || call.getKind() == SqlKind.AS) {
                return isSupportedProxyOperand(call.getOperands().get(0));
            }
        }
        return false;
    }

    private static String mergeProxyConf(@Nullable String existing, String conf) {
        if (conf == null || conf.isEmpty()) {
            return existing == null ? "" : existing;
        }
        if (existing == null || existing.isEmpty()) {
            return conf;
        }
        if (!existing.equals(conf)) {
            throw new TableException(
                    "Proxy scalar function requires a single, consistent conf literal.");
        }
        return existing;
    }

    private static String extractProxyConf(List<RexNode> operands) {
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

    public static final class ProxyScalarFunctionRewriter extends RexShuttle {
        private final String targetClassName;
        private final String targetSimpleName;
        private final RowType inputRowType;
        private final RowType outputRowType;
        private boolean proxyFunctionFound;
        private @Nullable String proxyConf;
        private boolean loggedFirstCall;
        private int currentOutputFieldIndex = -1;
        private @Nullable Integer currentUdfFieldIndexOverride;
        private final List<Integer> proxyArgFieldIndices = new ArrayList<>();
        private final List<String> proxyArgFieldNames = new ArrayList<>();
        private final List<String> proxyArgFieldTypes = new ArrayList<>();
        private final List<Integer> proxyResultFieldIndices = new ArrayList<>();
        private final List<String> proxyResultFieldNames = new ArrayList<>();
        private final List<String> proxyResultFieldTypes = new ArrayList<>();
        private final List<Integer> proxyResultUdfFieldIndices = new ArrayList<>();
        private final List<String> proxyResultUdfFieldTypes = new ArrayList<>();

        public ProxyScalarFunctionRewriter(
                String targetClassName, RowType inputRowType, RowType outputRowType) {
            this.targetClassName = checkNotNull(targetClassName, "targetClassName");
            this.targetSimpleName = deriveSimpleName(targetClassName);
            this.inputRowType = checkNotNull(inputRowType, "inputRowType");
            this.outputRowType = checkNotNull(outputRowType, "outputRowType");
        }

        public void setCurrentOutputFieldIndex(int outputFieldIndex) {
            this.currentOutputFieldIndex = outputFieldIndex;
        }

        @Override
        public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
            final RexCall proxyCall = unwrapProxyCall(fieldAccess.getReferenceExpr());
            if (proxyCall == null || fieldAccess.getField() == null) {
                return super.visitFieldAccess(fieldAccess);
            }
            final int udfFieldIndex = fieldAccess.getField().getIndex();
            currentUdfFieldIndexOverride = udfFieldIndex;
            try {
                // Drop the field access from the rewritten expression. The external proxy
                // will extract the correct field based on the recorded udfFieldIndex.
                return proxyCall.accept(this);
            } finally {
                currentUdfFieldIndexOverride = null;
            }
        }

        @Override
        public RexNode visitCall(RexCall call) {
            if (!loggedFirstCall) {
                loggedFirstCall = true;
                final SqlOperator operator = call.getOperator();
                final String opName = operator == null ? "<null>" : operator.getName();
                final String opClass =
                        operator == null ? "<null>" : operator.getClass().getName();
                LOG.info(
                        "Proxy rewrite first RexCall: opName={}, opClass={}, kind={}, operands={}",
                        opName,
                        opClass,
                        call.getKind(),
                        call.getOperands().size());
                if (operator instanceof BridgingSqlFunction) {
                    final BridgingSqlFunction bridging = (BridgingSqlFunction) operator;
                    final String identifierName =
                            bridging.getResolvedFunction()
                                    .getIdentifier()
                                    .map(FunctionIdentifier::getFunctionName)
                                    .orElse(null);
                    final FunctionDefinition definition = bridging.getDefinition();
                    LOG.info(
                            "Proxy rewrite first RexCall bridging: identifierName={}, definition={}",
                            identifierName,
                            definition == null ? "<null>" : definition.getClass().getName());
                } else if (operator instanceof ScalarSqlFunction) {
                    final ScalarSqlFunction scalar = (ScalarSqlFunction) operator;
                    LOG.info(
                            "Proxy rewrite first RexCall scalar: functionClass={}",
                            scalar.scalarFunction().getClass().getName());
                }
            }
            if (LOG.isDebugEnabled()) {
                final SqlOperator operator = call.getOperator();
                final String opName = operator == null ? "<null>" : operator.getName();
                final String opClass = operator == null ? "<null>" : operator.getClass().getName();
                LOG.debug(
                        "Proxy rewrite visiting RexCall: opName={}, opClass={}, kind={}, operands={}",
                        opName,
                        opClass,
                        call.getKind(),
                        call.getOperands().size());
                if (operator instanceof BridgingSqlFunction) {
                    final BridgingSqlFunction bridging = (BridgingSqlFunction) operator;
                    final String identifierName =
                            bridging.getResolvedFunction()
                                    .getIdentifier()
                                    .map(FunctionIdentifier::getFunctionName)
                                    .orElse(null);
                    final FunctionDefinition definition = bridging.getDefinition();
                    LOG.debug(
                            "Proxy rewrite BridgingSqlFunction details: identifierName={}, definition={}",
                            identifierName,
                            definition == null ? "<null>" : definition.getClass().getName());
                } else if (operator instanceof ScalarSqlFunction) {
                    final ScalarSqlFunction scalar = (ScalarSqlFunction) operator;
                    LOG.debug(
                            "Proxy rewrite ScalarSqlFunction details: functionClass={}",
                            scalar.scalarFunction().getClass().getName());
                }
            }
            if (!isProxyScalarFunction(call, targetClassName, targetSimpleName)) {
                return super.visitCall(call);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Proxy rewrite matched scalar UDF call: opName={}, opClass={}, targetClass={}",
                        call.getOperator().getName(),
                        call.getOperator().getClass().getName(),
                        targetClassName);
            }
            final List<RexNode> operands = call.getOperands();
            if (operands.isEmpty()) {
                throw new TableException("Proxy scalar function requires at least one argument.");
            }
            proxyFunctionFound = true;
            proxyConf = mergeProxyConf(proxyConf, extractProxyConf(operands));
            boolean foundFieldArg = false;
            RexNode firstFieldOperand = null;
            @Nullable Integer firstFieldIndex = null;
            for (RexNode operand : operands) {
                if (operand instanceof RexLiteral || operand.getKind() == SqlKind.DEFAULT) {
                    continue;
                }
                if (!isSupportedProxyOperand(operand)) {
                    throw new TableException(
                            "Proxy scalar function requires column references for all non-literal arguments.");
                }
                final @Nullable Integer fieldIndex = extractProxyFieldIndex(operand);
                if (fieldIndex == null) {
                    throw new TableException(
                            "Proxy scalar function requires input references as arguments.");
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
                        "Proxy scalar function requires at least one column reference argument.");
            }
            final String udfReturnType = resolveUdfReturnType(call, currentUdfFieldIndexOverride);
            addResultField(currentUdfFieldIndexOverride, firstFieldIndex, udfReturnType);
            return firstFieldOperand.accept(this);
        }

        public boolean hasProxyFunction() {
            return proxyFunctionFound;
        }

        public String getProxyConf() {
            return proxyConf == null ? "" : proxyConf;
        }

        public String getProxyFunctionClass() {
            return targetClassName;
        }

        public String getProxyFunctionKind() {
            return PROXY_FUNCTION_KIND_SCALAR;
        }

        public List<Integer> getProxyArgFieldIndices() {
            return proxyArgFieldIndices;
        }

        public List<String> getProxyArgFieldNames() {
            return proxyArgFieldNames;
        }

        public List<String> getProxyArgFieldTypes() {
            return proxyArgFieldTypes;
        }

        public List<Integer> getProxyResultFieldIndices() {
            return proxyResultFieldIndices;
        }

        public List<String> getProxyResultFieldNames() {
            return proxyResultFieldNames;
        }

        public List<String> getProxyResultFieldTypes() {
            return proxyResultFieldTypes;
        }

        public List<Integer> getProxyResultUdfFieldIndices() {
            return proxyResultUdfFieldIndices;
        }

        public List<String> getProxyResultUdfFieldTypes() {
            return proxyResultUdfFieldTypes;
        }

        private static String deriveSimpleName(String className) {
            final int lastDot = className.lastIndexOf('.');
            String simple = lastDot < 0 ? className : className.substring(lastDot + 1);
            final int suffixIndex = simple.indexOf('$');
            if (suffixIndex > 0) {
                simple = simple.substring(0, suffixIndex);
            }
            return simple;
        }

        private void addArgField(int fieldIndex) {
            proxyArgFieldIndices.add(fieldIndex);
            proxyArgFieldNames.add(resolveFieldName(inputRowType, fieldIndex));
            proxyArgFieldTypes.add(resolveFieldType(inputRowType, fieldIndex));
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
            if (proxyResultFieldIndices.contains(targetFieldIndex)) {
                return;
            }
            proxyResultFieldIndices.add(targetFieldIndex);
            proxyResultFieldNames.add(resolveFieldName(outputRowType, currentOutputFieldIndex));
            proxyResultFieldTypes.add(resolveFieldType(outputRowType, currentOutputFieldIndex));
            proxyResultUdfFieldIndices.add(udfFieldIndex == null ? -1 : udfFieldIndex);
            proxyResultUdfFieldTypes.add(udfFieldType);
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

        private @Nullable RexCall unwrapProxyCall(RexNode node) {
            if (!(node instanceof RexCall)) {
                return null;
            }
            final RexCall call = (RexCall) node;
            if (isProxyScalarFunction(call, targetClassName, targetSimpleName)) {
                return call;
            }
            if (call.getKind() == SqlKind.CAST || call.getKind() == SqlKind.AS) {
                return unwrapProxyCall(call.getOperands().get(0));
            }
            return null;
        }

        private void addArgField(RexNode operand, int fieldIndex) {
            proxyArgFieldIndices.add(fieldIndex);
            proxyArgFieldNames.add(resolveFieldName(inputRowType, fieldIndex));
            proxyArgFieldTypes.add(resolveOperandType(inputRowType, operand, fieldIndex));
        }
    }

    private static @Nullable Integer extractProxyFieldIndex(RexNode operand) {
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
                    return extractProxyFieldIndex(call.getOperands().get(0));
                }
            }
            return null;
        }
        if (operand instanceof RexCall) {
            final RexCall call = (RexCall) operand;
            if (call.getKind() == SqlKind.CAST || call.getKind() == SqlKind.AS) {
                return extractProxyFieldIndex(call.getOperands().get(0));
            }
        }
        return null;
    }

    private static String resolveFieldName(RowType rowType, int fieldIndex) {
        final List<RowType.RowField> fields = rowType.getFields();
        if (fieldIndex < 0 || fieldIndex >= fields.size()) {
            throw new TableException(
                    "Proxy scalar function argument index out of bounds: " + fieldIndex);
        }
        return fields.get(fieldIndex).getName();
    }

    private static String resolveFieldType(RowType rowType, int fieldIndex) {
        final List<RowType.RowField> fields = rowType.getFields();
        if (fieldIndex < 0 || fieldIndex >= fields.size()) {
            throw new TableException(
                    "Proxy scalar function argument index out of bounds: " + fieldIndex);
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
