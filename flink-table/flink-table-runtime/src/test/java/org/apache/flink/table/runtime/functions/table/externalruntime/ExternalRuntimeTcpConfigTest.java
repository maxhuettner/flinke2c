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

package org.apache.flink.table.runtime.functions.table.externalruntime;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ExternalRuntimeTcpConfig}. */
class ExternalRuntimeTcpConfigTest {

    @Test
    void testParseAutoParallelAndFailover() {
        final ExternalRuntimeTcpConfig config =
                ExternalRuntimeTcpConfig.from(
                        "runtimes=host1:9000,host2:9001;parallel=auto;autofailover=true");

        assertThat(config.isAutoParallelismEnabled()).isTrue();
        assertThat(config.isAutoFailoverEnabled()).isTrue();
        assertThat(config.getRuntimeParallelism()).isEqualTo(0);
        assertThat(config.selectEndpoints(0, 1)).hasSize(2);
    }

    @Test
    void testParseParallelAliasAsFixedParallelism() {
        final ExternalRuntimeTcpConfig config =
                ExternalRuntimeTcpConfig.from("runtimes=host1:9000,host2:9001;parallel=1");

        assertThat(config.isAutoParallelismEnabled()).isFalse();
        assertThat(config.isAutoFailoverEnabled()).isFalse();
        assertThat(config.getRuntimeParallelism()).isEqualTo(1);
        assertThat(config.selectEndpoints(0, 1)).hasSize(1);
    }

    @Test
    void testAutoParallelismFlagOverridesDefault() {
        final ExternalRuntimeTcpConfig config =
                ExternalRuntimeTcpConfig.from(
                        "runtimes=host1:9000,host2:9001,host3:9002;"
                                + "parallel=2;autoparallelism=true");

        assertThat(config.isAutoParallelismEnabled()).isTrue();
        assertThat(config.getRuntimeParallelism()).isEqualTo(2);
        assertThat(config.selectEndpoints(0, 1)).hasSize(2);
    }
}

