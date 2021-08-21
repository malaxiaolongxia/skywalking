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
import com.fasterxml.jackson.core.io.SerializedString;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import org.apache.skywalking.library.elasticsearch.requests.UpdateRequest;

final class UpdateRequestSerializer extends StdSerializer<UpdateRequest> {
    protected UpdateRequestSerializer() {
        super(UpdateRequest.class);
    }

    @Override
    public void serialize(final UpdateRequest value, final JsonGenerator gen,
                          final SerializerProvider provider) throws IOException {
        gen.setRootValueSeparator(new SerializedString("\n"));

        gen.writeStartObject();
        {
            gen.writeFieldName("update");
            gen.writeStartObject();
            {
                gen.writeStringField("_index", value.getIndex());
                gen.writeStringField("_type", value.getType());
                gen.writeStringField("_id", value.getId());
            }
            gen.writeEndObject();
        }

        gen.writeEndObject();

        gen.writeStartObject();
        {
            gen.writeFieldName("doc");
            gen.writeObject(value.getDoc());
        }
        gen.writeEndObject();
    }
}
