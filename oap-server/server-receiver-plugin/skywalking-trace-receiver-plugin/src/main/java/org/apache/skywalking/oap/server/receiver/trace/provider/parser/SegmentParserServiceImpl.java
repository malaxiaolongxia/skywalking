/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.skywalking.oap.server.receiver.trace.provider.parser;

import org.apache.skywalking.apm.network.language.agent.v3.SegmentObject;
import org.apache.skywalking.oap.server.library.module.ModuleManager;
import org.apache.skywalking.oap.server.receiver.trace.provider.TraceServiceModuleConfig;

/**
 * The open service to the receivers.
 */
public class SegmentParserServiceImpl implements ISegmentParserService {
    private ModuleManager moduleManager;
    private SegmentParserListenerManager listenerManager;
    private TraceServiceModuleConfig config;

    public SegmentParserServiceImpl(ModuleManager moduleManager,
                                    SegmentParserListenerManager listenerManager,
                                    TraceServiceModuleConfig config) {
        this.moduleManager = moduleManager;
        this.listenerManager = listenerManager;
        this.config = config;
    }

    @Override
    public void send(SegmentObject segment) {
        final TraceAnalyzer traceAnalyzer = new TraceAnalyzer(moduleManager, listenerManager, config);
        traceAnalyzer.doAnalysis(segment);
    }
}
