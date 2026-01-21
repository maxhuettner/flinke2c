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
import org.apache.flink.table.types.logical.RowType;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;


/** Base class for exec Calc. */
public abstract class CommonExecCalc extends ExecNodeBase<RowData>
        implements SingleTransformationTranslator<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(CommonExecCalc.class);

    public static final String CALC_TRANSFORMATION = "calc";

    protected static final String CUSTOM_PROXY_FUNCTION_NAME =
            "CurrencyConversionFunction";

    protected static final String CUSTOM_PROXY_FUNCTION_CLASS_NAME =
            "org.example.flinke2c.CurrencyConversionFunction";

    protected static final ConfigOption<String> PROXY_CONF_OPTION =
            ConfigOptions.key("table.exec.proxy.conf").stringType().noDefaultValue();
    public static final ConfigOption<Boolean> PROXY_CHAIN_ONLY_OPTION =
            ConfigOptions.key("table.exec.proxy.chain-only.enabled")
                    .booleanType()
                    .defaultValue(false);

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

        final ProxyScalarFunctionRewriter rewriter = new ProxyScalarFunctionRewriter();
        final List<RexNode> rewrittenProjection = new ArrayList<>(projection.size());
        for (RexNode node : projection) {
            rewrittenProjection.add(node.accept(rewriter));
        }
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
            LOG.info(
                    "Proxy rewrite injecting pre/post operators for scalar UDF: {}, conf={}",
                    CUSTOM_PROXY_FUNCTION_NAME,
                    proxyConf);
            resolvedProxyConf = proxyConf;
        } else {
            resolvedProxyConf = null;
        }

        final CodeGeneratorContext ctx =
                new CodeGeneratorContext(config, planner.getFlinkContext().getClassLoader())
                        .setOperatorBaseClass(operatorBaseClass);

        final CodeGenOperatorFactory<RowData> substituteStreamOperator =
                CalcCodeGenerator.generateCalcOperator(
                        ctx,
                        inputTransform,
                        (RowType) getOutputType(),
                        JavaScalaConversionUtil.toScala(effectiveProjection),
                        JavaScalaConversionUtil.toScala(Optional.ofNullable(effectiveCondition)),
                        retainHeader,
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
            if (Boolean.TRUE.equals(config.get(PROXY_CHAIN_ONLY_OPTION)) && calcTransform instanceof PhysicalTransformation) {
                ((PhysicalTransformation<?>) calcTransform)
                        .setChainingStrategy(ChainingStrategy.HEAD);
            }
            return createProxyChain(calcTransform, resolvedProxyConf, config);
        }
        return calcTransform;
    }

    protected Transformation<RowData> createProxyChain(
            Transformation<RowData> input, String conf, ExecNodeConfig config) {
        final RowType rowType = extractRowType(input);
        final OneInputTransformation<RowData, RowData> pre =
                ExecNodeUtil.createOneInputTransformation(
                        input,
                        createTransformationMeta("proxy-pre", "ProxyPre", "ProxyPre", config),
                        new ProxyOperator(conf, ProxyOperator.Side.PRE, rowType),
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
                        new ProxyOperator(conf, ProxyOperator.Side.POST, rowType),
                        pre.getOutputType(),
                        pre.getParallelism(),
                        pre.isParallelismConfigured());
        copyPlacementConstraints(input, post);
        if (Boolean.TRUE.equals(config.get(PROXY_CHAIN_ONLY_OPTION))) {
            post.setChainingStrategy(ChainingStrategy.HEAD);
        }
        setMaxParallelismIfConfigured(input, post);

        return post;
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

    private static boolean matchesOperatorName(@Nullable String name) {
        if (name == null) {
            return false;
        }
        final String trimmed = name.trim();
        if (trimmed.isEmpty()) {
            return false;
        }
        final String normalized = trimmed.replace("\"", "").replace("`", "");
        if (normalized.equalsIgnoreCase(CUSTOM_PROXY_FUNCTION_NAME)) {
            return true;
        }
        final int lastDot = normalized.lastIndexOf('.');
        if (lastDot >= 0 && lastDot < normalized.length() - 1) {
            final String simple = normalized.substring(lastDot + 1);
            if (simple.equalsIgnoreCase(CUSTOM_PROXY_FUNCTION_NAME)) {
                return true;
            }
        }
        final int suffixIndex = normalized.indexOf('$');
        if (suffixIndex > 0) {
            return normalized.substring(0, suffixIndex)
                    .equalsIgnoreCase(CUSTOM_PROXY_FUNCTION_NAME);
        }
        return false;
    }

    private static boolean isProxyScalarFunction(RexCall call) {
        final SqlOperator operator = call.getOperator();
        if (matchesOperatorName(operator.getName())) {
            return true;
        }
        if (operator instanceof ScalarSqlFunction) {
            final ScalarSqlFunction function = (ScalarSqlFunction) operator;
            return CUSTOM_PROXY_FUNCTION_CLASS_NAME.equals(
                    function.scalarFunction().getClass().getName());
        }
        if (operator instanceof BridgingSqlFunction) {
            final BridgingSqlFunction bridging = (BridgingSqlFunction) operator;
            final String identifierName =
                    bridging.getResolvedFunction()
                            .getIdentifier()
                            .map(FunctionIdentifier::getFunctionName)
                            .orElse(null);
            if (matchesOperatorName(identifierName)) {
                return true;
            }
            final FunctionDefinition definition = bridging.getDefinition();
            return matchesDefinition(definition);
        }
        return false;
    }

    private static boolean matchesDefinition(@Nullable FunctionDefinition definition) {
        if (definition == null) {
            return false;
        }
        if (definition instanceof ScalarFunctionDefinition) {
            final ScalarFunction scalarFunction =
                    ((ScalarFunctionDefinition) definition).getScalarFunction();
            return CUSTOM_PROXY_FUNCTION_CLASS_NAME.equals(
                    scalarFunction.getClass().getName());
        }
        if (definition instanceof ScalarFunction) {
            return CUSTOM_PROXY_FUNCTION_CLASS_NAME.equals(definition.getClass().getName());
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
        private boolean proxyFunctionFound;
        private @Nullable String proxyConf;
        private boolean loggedFirstCall;

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
            if (!isProxyScalarFunction(call)) {
                return super.visitCall(call);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Proxy rewrite matched scalar UDF call: opName={}, opClass={}",
                        call.getOperator().getName(),
                        call.getOperator().getClass().getName());
            }
            final List<RexNode> operands = call.getOperands();
            if (operands.isEmpty()) {
                throw new TableException("Proxy scalar function requires at least one argument.");
            }
            final RexNode operand = operands.get(0);
            if (!isSupportedProxyOperand(operand)) {
                throw new TableException(
                        "Proxy scalar function requires a column reference as its first argument.");
            }
            proxyFunctionFound = true;
            proxyConf = mergeProxyConf(proxyConf, extractProxyConf(operands));
            return operand.accept(this);
        }

        public boolean hasProxyFunction() {
            return proxyFunctionFound;
        }

        public String getProxyConf() {
            return proxyConf == null ? "" : proxyConf;
        }
    }

}
