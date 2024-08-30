/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.protobuf;

import com.google.protobuf.ByteString;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TotalHits;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.proto.search.SearchHitsProtoDef.SearchHitsProto;
import org.opensearch.proto.search.SearchHitsProtoDef.TotalHitsProto;

import java.io.IOException;

/**
 * SearchHits child which implements serde operations as protobuf.
 * @opensearch.internal
 */
public class SearchHitsProtobuf extends SearchHits {
    public SearchHitsProtobuf(SearchHits hits) {
        super(hits);
    }

    public SearchHitsProtobuf(StreamInput in) throws IOException {
        fromProtobufStream(in);
    }

    public SearchHitsProtobuf(SearchHitsProto proto) {
        fromProto(proto);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        toProtobufStream(out);
    }

    public void toProtobufStream(StreamOutput out) throws IOException {
        toProto().writeTo(out);
    }

    public void fromProtobufStream(StreamInput in) throws IOException {
        SearchHitsProto proto = SearchHitsProto.parseFrom(in);
        fromProto(proto);
    }

    SearchHitsProto toProto() {
        SearchHitsProto.Builder builder = SearchHitsProto.newBuilder().setMaxScore(maxScore);

        for (SearchHit hit : hits) {
            builder.addHits(new SearchHitProtobuf(hit).toProto());
        }

        if (collapseField != null) {
            builder.setCollapseField(collapseField);
        }

        if (totalHits != null) {
            TotalHitsProto.Builder totHitsBuilder = TotalHitsProto.newBuilder()
                .setRelation(totalHits.relation.ordinal())
                .setValue(totalHits.value);
            builder.setTotalHits(totHitsBuilder);
        }

        try (BytesStreamOutput sortOut = new BytesStreamOutput()) {
            sortOut.writeOptionalArray(Lucene::writeSortField, sortFields);
            builder.setSortFields(ByteString.copyFrom(sortOut.bytes().toBytesRef().bytes));
        } catch (IOException e) {
            throw new ProtoSerDeHelpers.SerializationException("Failed to serialize SearchHits to proto", e);
        }

        try (BytesStreamOutput collapseOut = new BytesStreamOutput()) {
            collapseOut.writeOptionalArray(Lucene::writeSortValue, collapseValues);
            builder.setCollapseValues(ByteString.copyFrom(collapseOut.bytes().toBytesRef().bytes));
        } catch (IOException e) {
            throw new ProtoSerDeHelpers.SerializationException("Failed to serialize SearchHits to proto", e);
        }

        return builder.build();
    }

    void fromProto(SearchHitsProto proto) throws ProtoSerDeHelpers.SerializationException {
        maxScore = proto.getMaxScore();
        collapseField = proto.getCollapseField();

        if (proto.hasTotalHits()) {
            long rel = proto.getTotalHits().getRelation();
            long val = proto.getTotalHits().getValue();
            if (rel < 0 || rel >= TotalHits.Relation.values().length) {
                throw new ProtoSerDeHelpers.SerializationException("Failed to deserialize TotalHits from proto");
            }
            totalHits = new TotalHits(val, TotalHits.Relation.values()[(int) rel]);
        } else {
            totalHits = null;
        }

        try (StreamInput sortBytesInput = new BytesArray(proto.getSortFields().toByteArray()).streamInput()) {
            sortFields = sortBytesInput.readOptionalArray(Lucene::readSortField, SortField[]::new);
        } catch (IOException e) {
            throw new ProtoSerDeHelpers.SerializationException("Failed to deserialize SearchHits from proto", e);
        }

        try (StreamInput collapseBytesInput = new BytesArray(proto.getCollapseValues().toByteArray()).streamInput()) {
            collapseValues = collapseBytesInput.readOptionalArray(Lucene::readSortValue, Object[]::new);
        } catch (IOException e) {
            throw new ProtoSerDeHelpers.SerializationException("Failed to deserialize SearchHits from proto", e);
        }

        hits = new SearchHit[proto.getHitsCount()];
        for (int i = 0; i < hits.length; i++) {
            hits[i] = new SearchHitProtobuf(proto.getHits(i));
        }
    }
}
