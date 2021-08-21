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
 */

package org.apache.skywalking.library.elasticsearch.requests.search;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Represents criteria when matching documents in ElasticSearch.
 */
public abstract class Query {
    public static RangeQueryBuilder range(String name) {
        checkArgument(!Strings.isNullOrEmpty(name), "name cannot be blank");
        return new RangeQueryBuilder(name);
    }

    public static TermQuery term(String name, Object value) {
        checkArgument(!Strings.isNullOrEmpty(name), "name cannot be blank");
        requireNonNull(value, "value");
        return new TermQuery(name, value);
    }

    public static MatchQuery match(String name, String text) {
        checkArgument(!Strings.isNullOrEmpty(name), "name cannot be blank");
        checkArgument(!Strings.isNullOrEmpty(text), "text cannot be blank");
        return new MatchQuery(name, text);
    }

    public static MatchPhaseQuery matchPhrase(String name, String text) {
        checkArgument(!Strings.isNullOrEmpty(name), "name cannot be blank");
        checkArgument(!Strings.isNullOrEmpty(text), "text cannot be blank");
        return new MatchPhaseQuery(name, text);
    }

    public static IdsQuery ids(String... ids) {
        requireNonNull(ids, "ids");
        checkArgument(ids.length > 0, "ids cannot be empty");
        return ids(Arrays.asList(ids));
    }

    public static IdsQuery ids(Iterable<String> ids) {
        requireNonNull(ids, "ids");
        return new IdsQuery(ImmutableList.<String>builder().addAll(ids).build());
    }

    public static BoolQueryBuilder bool() {
        return new BoolQueryBuilder();
    }
}
