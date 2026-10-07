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

package org.apache.flink.table.runtime.functions.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.types.inference.StaticArgument;
import org.apache.flink.table.types.inference.StaticArgumentTrait;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.types.Row;

import java.util.EnumSet;
import java.util.Optional;

import static org.apache.flink.table.annotation.ArgumentTrait.ROW_SEMANTIC_TABLE;

/**
 * External runtime marker PTF that preserves input schema.
 *
 * <p>This function is used as a hook for planner rewrites and is not expected to run
 * in production.
 */
@Internal
public class ExternalRuntimeTableFunction extends ProcessTableFunction<Row> {

    public void eval(
            @ArgumentHint(value = ROW_SEMANTIC_TABLE, name = "r") Row r,
            @ArgumentHint(name = "conf", isOptional = true) String conf) {
        collect(r);
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                .staticArguments(
                        StaticArgument.table(
                                "r",
                                Row.class,
                                false,
                                EnumSet.of(StaticArgumentTrait.ROW_SEMANTIC_TABLE)),
                        StaticArgument.scalar("conf", DataTypes.STRING().nullable(), true))
                .outputTypeStrategy(
                        callContext -> Optional.of(callContext.getArgumentDataTypes().get(0)))
                .build();
    }
}
