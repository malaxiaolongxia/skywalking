/*
 * Copyright 2017, OpenSkywalking Organization All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Project repository: https://github.com/OpenSkywalking/skywalking
 */

package org.skywalking.apm.collector.core.module;

import java.util.Map;
import org.skywalking.apm.collector.core.CollectorException;

/**
 * @author pengys5
 */
public abstract class CommonModuleInstaller implements ModuleInstaller {

    private boolean isInstalled = false;
    private Map<String, Map> moduleConfig;
    private Map<String, ModuleDefine> moduleDefineMap;

    @Override
    public final void injectConfiguration(Map<String, Map> moduleConfig, Map<String, ModuleDefine> moduleDefineMap) {
        this.moduleConfig = moduleConfig;
        this.moduleDefineMap = moduleDefineMap;
    }

    protected final Map<String, Map> getModuleConfig() {
        return moduleConfig;
    }

    protected final Map<String, ModuleDefine> getModuleDefineMap() {
        return moduleDefineMap;
    }

    public abstract void onAfterInstall() throws CollectorException;

    @Override public final void afterInstall() throws CollectorException {
        if (!isInstalled) {
            onAfterInstall();
        }
        isInstalled = true;
    }
}
