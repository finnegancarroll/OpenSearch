/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.search;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.io.InputStream;
import java.nio.file.*;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.proto.search.SearchHitsProtoDef.SearchHitsProto;
import org.opensearch.search.SearchHits;
import org.opensearch.transport.protobuf.SearchHitsProtobuf;

@Warmup(iterations = 1)
@Measurement(iterations = 2, time = 60, timeUnit = TimeUnit.SECONDS)
@Fork(2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class SearchHitsProtobufBenchmark {
    String READ_PATH = "/tmp/testFiles";
    int TEST_FILES = 400;

    // Setup proto and native representations of SearchHits
    List<SearchHitsProto> searchHitsProtoItems = new ArrayList<>();
    List<SearchHits> searchHitsNativePojoItems = new ArrayList<>();

    /*
    NOTE: Test items are read from disk. Randomized SearchHits are generated with SearchHitsTests.createTestItem().

    To generate test items and run all microbenchmarks in this class:
    ./gradlew server:test --tests "org.opensearch.search.SearchHitsTests.testMicroBenchmarkHackGenerateTestFiles" -Dtests.security.manager=false
    ./gradlew -p benchmarks run --args 'SearchHitsProtobufBenchmark'
     */

    @Setup
    public void setup() throws IOException {
        Path dir = Paths.get(READ_PATH);

        for(int i = 0; i < TEST_FILES; i++) {
            Path testFile = dir.resolve("testItem_" + i);
            try (InputStream in = Files.newInputStream(testFile)) {
                BytesStreamInput sin = new BytesStreamInput(in.readAllBytes());

                SearchHits sh = new SearchHits(sin);
                SearchHitsProto shProto = new SearchHitsProtobuf(sh).toProto();

                searchHitsNativePojoItems.add(sh);
                searchHitsProtoItems.add(shProto);
            }
        }
    }

    /*
    ./gradlew -p benchmarks run --args 'SearchHitsProtobufBenchmark.writeToNativeBench'
     */
    @Benchmark
    public BytesStreamOutput writeToNativeBench() throws IOException {
        BytesStreamOutput bytes = new BytesStreamOutput();
        for (SearchHits sh : searchHitsNativePojoItems) {
            sh.writeTo(bytes);
        }
        return bytes;
    }

    /*
    ./gradlew -p benchmarks run --args 'SearchHitsProtobufBenchmark.writeToProtoBench'
     */
    @Benchmark
    public BytesStreamOutput writeToProtoBench() throws IOException {
        BytesStreamOutput bytes = new BytesStreamOutput();
        for (SearchHitsProto shProto : searchHitsProtoItems) {
            shProto.writeTo(bytes);
        }
        return bytes;
    }

    /*
    ./gradlew -p benchmarks run --args 'SearchHitsProtobufBenchmark.toXContNativeBench'
     */
    @Benchmark
    public List<XContentBuilder> toXContNativeBench() throws IOException {
        List<XContentBuilder> XContList = new ArrayList<>();
        for (SearchHits sh : searchHitsNativePojoItems) {
            XContentBuilder builder = JsonXContent.contentBuilder();
            builder.startObject();
            sh.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            XContList.add(builder);
        }

        return XContList;
    }

    /*
    ./gradlew -p benchmarks run --args 'SearchHitsProtobufBenchmark.toXContProtoBench'
     */
    @Benchmark
    public List<XContentBuilder> toXContProtoBench() throws IOException {
        List<XContentBuilder> XContList = new ArrayList<>();
        for (SearchHitsProto sh : searchHitsProtoItems) {
            XContentBuilder builder = JsonXContent.contentBuilder();
            /*
            Removing JsonXContentGenerator.java 'if (mayWriteRawData(mediaType) == false) {' check
            "application/octet-stream" media type not registered.
             */
            builder.startObject();
            builder.rawField("protobuf", sh.toByteString().newInput(), MediaType.fromMediaType("application/octet-stream"));
            builder.endObject();
            XContList.add(builder);
        }

        return XContList;
    }
}
