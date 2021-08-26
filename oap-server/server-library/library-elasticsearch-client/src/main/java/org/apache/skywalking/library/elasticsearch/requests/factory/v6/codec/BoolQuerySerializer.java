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

package org.apache.skywalking.library.elasticsearch.requests.factory.v6.codec;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import java.util.List;
import org.apache.skywalking.library.elasticsearch.requests.search.BoolQuery;
import org.apache.skywalking.library.elasticsearch.requests.search.Query;

public final class BoolQuerySerializer extends JsonSerializer<BoolQuery> {
    static final String NAME = "bool";
    static final String MUST = "must";
    static final String MUST_NOT = "must_not";
    static final String SHOULD = "should";
    static final String SHOULD_NOT = "should_not";

    @Override
    public void serialize(final BoolQuery value, final JsonGenerator gen,
                          final SerializerProvider provider) throws IOException {
        gen.writeStartObject();
        {
            gen.writeFieldName(NAME);
            gen.writeStartObject();
            {
                writeArray(gen, MUST, value.getMust());
                writeArray(gen, SHOULD, value.getShould());
            }
            gen.writeEndObject();
        }
        gen.writeEndObject();
    }

    private void writeArray(final JsonGenerator gen, final String name,
                            final List<Query> array) throws IOException {
        if (array == null) {
            return;
        }

        gen.writeFieldName(name);
        gen.writeStartArray();
        {
            for (final Query query : array) {
                gen.writeObject(query);
            }
        }
        gen.writeEndArray();
    }
}
