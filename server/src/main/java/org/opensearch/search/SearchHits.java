/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search;

import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.TotalHits.Relation;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.proto.search.SearchHitsProtoDef;
import org.opensearch.rest.action.search.RestSearchAction;
import org.opensearch.transport.protobuf.SearchHitProtobuf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.transport.protobuf.ProtoSerDeHelpers.sortFieldToProto;
import static org.opensearch.transport.protobuf.SearchHitProtobuf.sortValueToProto;

/**
 * Encapsulates the results of a search operation
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class SearchHits implements Writeable, ToXContentFragment, Iterable<SearchHit> {
    public static SearchHits empty() {
        return empty(true);
    }

    public static SearchHits empty(boolean withTotalHits) {
        return new SearchHits(EMPTY, withTotalHits ? new TotalHits(0, Relation.EQUAL_TO) : null, 0);
    }

    public static final SearchHit[] EMPTY = new SearchHit[0];

    protected SearchHit[] hits;
    protected float maxScore;
    @Nullable
    protected TotalHits totalHits;
    @Nullable
    protected SortField[] sortFields;
    @Nullable
    protected String collapseField;
    @Nullable
    protected Object[] collapseValues;

    public SearchHits(SearchHits sHits) {
        this.hits = sHits.hits;
        this.totalHits = sHits.totalHits;
        this.maxScore = sHits.maxScore;
        this.sortFields = sHits.sortFields;
        this.collapseField = sHits.collapseField;
        this.collapseValues = sHits.collapseValues;
    }

    public SearchHits(SearchHit[] hits, @Nullable TotalHits totalHits, float maxScore) {
        this(hits, totalHits, maxScore, null, null, null);
    }

    public SearchHits(
        SearchHit[] hits,
        @Nullable TotalHits totalHits,
        float maxScore,
        @Nullable SortField[] sortFields,
        @Nullable String collapseField,
        @Nullable Object[] collapseValues
    ) {
        this.hits = hits;
        this.totalHits = totalHits;
        this.maxScore = maxScore;
        this.sortFields = sortFields;
        this.collapseField = collapseField;
        this.collapseValues = collapseValues;
    }

    public SearchHits(StreamInput in) throws IOException {
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
    }

    protected SearchHits() {}

    @Override
    public void writeTo(StreamOutput out) throws IOException {
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

    /**
     * The total number of hits for the query or null if the tracking of total hits
     * is disabled in the request.
     */
    @Nullable
    public TotalHits getTotalHits() {
        return totalHits;
    }

    /**
     * The maximum score of this query.
     */
    public float getMaxScore() {
        return maxScore;
    }

    /**
     * The hits of the search request (based on the search type, and from / size provided).
     */
    public SearchHit[] getHits() {
        return this.hits;
    }

    /**
     * Return the hit as the provided position.
     */
    public SearchHit getAt(int position) {
        return hits[position];
    }

    /**
     * In case documents were sorted by field(s), returns information about such field(s), null otherwise
     * @see SortField
     */
    @Nullable
    public SortField[] getSortFields() {
        return sortFields;
    }

    /**
     * In case field collapsing was performed, returns the field used for field collapsing, null otherwise
     */
    @Nullable
    public String getCollapseField() {
        return collapseField;
    }

    /**
     * In case field collapsing was performed, returns the values of the field that field collapsing was performed on, null otherwise
     */
    @Nullable
    public Object[] getCollapseValues() {
        return collapseValues;
    }

    @Override
    public Iterator<SearchHit> iterator() {
        return Arrays.stream(getHits()).iterator();
    }

    /**
     * Fields for parsing and toXContent
     *
     * @opensearch.internal
     */
    public static final class Fields {
        public static final String HITS = "hits";
        public static final String TOTAL = "total";
        public static final String MAX_SCORE = "max_score";
    }


    ////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////

    public SearchHitsProtoDef.SearchHitsProto toProto() {
        SearchHitsProtoDef.SearchHitsProto.Builder builder = SearchHitsProtoDef.SearchHitsProto.newBuilder().setMaxScore(maxScore);

        for (SearchHit hit : hits) {
            builder.addHits(new SearchHitProtobuf(hit).toProto());
        }

        if (collapseField != null) {
            builder.setCollapseField(collapseField);
        }

        if (totalHits != null) {
            SearchHitsProtoDef.TotalHitsProto.Builder totHitsBuilder = SearchHitsProtoDef.TotalHitsProto.newBuilder()
                .setRelation(totalHits.relation.ordinal())
                .setValue(totalHits.value);
            builder.setTotalHits(totHitsBuilder);
        }

        if (sortFields != null) {
            for (SortField field : sortFields) {
                builder.addSortFields(sortFieldToProto(field));
            }
        }

        if (collapseValues != null) {
            for (Object col : collapseValues) {
                builder.addCollapseValues(sortValueToProto(col));
            }
        }

        return builder.build();
    }


    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
//        System.out.println("SearchHits.toXContent");

        builder.startObject(Fields.HITS);

        builder.rawField("protobuf", toProto().toByteString().newInput(), MediaType.fromMediaType("application/octet-stream"));

        builder.endObject();
        return builder;
    }


    ////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////


    public static SearchHits fromXContent(XContentParser parser) throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            parser.nextToken();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        }
        XContentParser.Token token = parser.currentToken();
        String currentFieldName = null;
        List<SearchHit> hits = new ArrayList<>();
        TotalHits totalHits = null;
        float maxScore = 0f;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (Fields.TOTAL.equals(currentFieldName)) {
                    // For BWC with nodes pre 7.0
                    long value = parser.longValue();
                    totalHits = value == -1 ? null : new TotalHits(value, Relation.EQUAL_TO);
                } else if (Fields.MAX_SCORE.equals(currentFieldName)) {
                    maxScore = parser.floatValue();
                }
            } else if (token == XContentParser.Token.VALUE_NULL) {
                if (Fields.MAX_SCORE.equals(currentFieldName)) {
                    maxScore = Float.NaN; // NaN gets rendered as null-field
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (Fields.HITS.equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        hits.add(SearchHit.fromXContent(parser));
                    }
                } else {
                    parser.skipChildren();
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (Fields.TOTAL.equals(currentFieldName)) {
                    totalHits = parseTotalHitsFragment(parser);
                } else {
                    parser.skipChildren();
                }
            }
        }
        return new SearchHits(hits.toArray(new SearchHit[0]), totalHits, maxScore);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof SearchHits)) {
            return false;
        }
        SearchHits other = (SearchHits) obj;
        return Objects.equals(totalHits, other.totalHits)
            && Objects.equals(maxScore, other.maxScore)
            && Arrays.equals(hits, other.hits)
            && Arrays.equals(sortFields, other.sortFields)
            && Objects.equals(collapseField, other.collapseField)
            && Arrays.equals(collapseValues, other.collapseValues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            totalHits,
            maxScore,
            Arrays.hashCode(hits),
            Arrays.hashCode(sortFields),
            collapseField,
            Arrays.hashCode(collapseValues)
        );
    }

    public static TotalHits parseTotalHitsFragment(XContentParser parser) throws IOException {
        long value = -1;
        Relation relation = null;
        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("value".equals(currentFieldName)) {
                    value = parser.longValue();
                } else if ("relation".equals(currentFieldName)) {
                    relation = parseRelation(parser.text());
                }
            } else {
                parser.skipChildren();
            }
        }
        return new TotalHits(value, relation);
    }

    private static Relation parseRelation(String relation) {
        if ("gte".equals(relation)) {
            return Relation.GREATER_THAN_OR_EQUAL_TO;
        } else if ("eq".equals(relation)) {
            return Relation.EQUAL_TO;
        } else {
            throw new IllegalArgumentException("invalid total hits relation: " + relation);
        }
    }
}
