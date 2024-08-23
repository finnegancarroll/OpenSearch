/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.fetch.serde;

import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TotalHits;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;

import java.io.IOException;

import static org.opensearch.search.SearchHits.EMPTY;

/**
 * Serialization/Deserialization implementations for SearchHits.
 * @opensearch.internal
 */
public class SearchHitsSerDe implements SerDe.StreamSerializer<SearchHits>, SerDe.StreamDeserializer<SearchHits> {
    SearchHitSerDe searchHitSerDe;

    @Override
    public SearchHits deserialize(StreamInput in) {
        try {
            return fromStream(in);
        } catch (IOException e) {
            throw new SerDe.SerializationException("Failed to deserialize FetchSearchResult", e);
        }
    }

    @Override
    public void serialize(SearchHits object, StreamOutput out) throws SerDe.SerializationException {
        try {
            toStream(object, out);
        } catch (IOException e) {
            throw new SerDe.SerializationException("Failed to serialize FetchSearchResult", e);
        }
    }

    private SearchHits fromStream(StreamInput in) throws IOException {
        SearchHit[] hits;
        TotalHits totalHits;
        float maxScore;
        SortField[] sortFields;
        String collapseField;
        Object[] collapseValues;

        if (in.readBoolean()) {
            totalHits = Lucene.readTotalHits(in);
        } else {
            // track_total_hits is false
            totalHits = null;
        }
        maxScore = in.readFloat();
        int size = in.readVInt();
        if (size == 0) {
            hits = EMPTY;
        } else {
            hits = new SearchHit[size];
            for (int i = 0; i < hits.length; i++) {
                hits[i] = new SearchHit(in);
            }
        }
        sortFields = in.readOptionalArray(Lucene::readSortField, SortField[]::new);
        collapseField = in.readOptionalString();
        collapseValues = in.readOptionalArray(Lucene::readSortValue, Object[]::new);

        return new SearchHits(hits, totalHits, maxScore, sortFields, collapseField, collapseValues);
    }

    private void toStream(SearchHits object, StreamOutput out) throws IOException {
        SearchHits.SerializationAccess serI = object.getSerAccess();
        SearchHit[] hits = serI.getHits();
        TotalHits totalHits = serI.getTotalHits();
        float maxScore = serI.getMaxScore();
        SortField[] sortFields = serI.getSortFields();
        String collapseField = serI.getCollapseField();
        Object[] collapseValues = serI.getCollapseValues();

        final boolean hasTotalHits = totalHits != null;
        out.writeBoolean(hasTotalHits);
        if (hasTotalHits) {
            Lucene.writeTotalHits(out, totalHits);
        }
        out.writeFloat(maxScore);
        out.writeVInt(hits.length);
        if (hits.length > 0) {
            for (SearchHit hit : hits) {
                hit.writeTo(out);
            }
        }
        out.writeOptionalArray(Lucene::writeSortField, sortFields);
        out.writeOptionalString(collapseField);
        out.writeOptionalArray(Lucene::writeSortValue, collapseValues);
    }
}