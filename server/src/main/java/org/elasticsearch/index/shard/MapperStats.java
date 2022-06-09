/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Optional;

public class MapperStats implements Writeable, ToXContentFragment {

    private String source;

    public MapperStats(String source) {
        this.source = source;
    }

    public MapperStats(StreamInput in) throws IOException {
        assert in.getVersion().onOrAfter(Version.V_8_4_0) : in.getVersion();
        this.source = in.readString();
    }


    @Override
    public void writeTo(StreamOutput out) throws IOException {
        assert out.getVersion().onOrAfter(Version.V_8_4_0) : out.getVersion();
        out.writeString(source);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("mapper");
        builder.field("source", source);
        builder.endObject();
        return builder;
    }

    public static MapperStats max(MapperStats a, MapperStats b) {
        return new MapperStats("max(" + Optional.ofNullable(a).map(s -> s.source).orElse("null") + ","
                               + Optional.ofNullable(b).map(s -> s.source).orElse("null") + ")");

//        if (a == null) {
//            return b;
//        }
//        if (b == null) {
//            return a;
//        }
//
//        // TODO
//        return new MapperStats("max(" a.source);
    }

    public void add(MapperStats other) {
        source = source + " + " + other;
    }
}
