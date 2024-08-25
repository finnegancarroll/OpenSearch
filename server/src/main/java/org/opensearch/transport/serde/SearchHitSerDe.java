/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.serde;

import com.google.protobuf.ByteString;
import org.apache.lucene.search.Explanation;
import org.opensearch.Version;
import org.opensearch.common.document.DocumentField;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.text.Text;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.SearchSortValues;
import org.opensearch.search.fetch.subphase.highlight.HighlightField;
import org.opensearch.serde.proto.SearchHitsTransportProto.SearchHitsProto;
import org.opensearch.serde.proto.SearchHitsTransportProto.SearchHitProto;
import org.opensearch.serde.proto.SearchHitsTransportProto.NestedIdentityProto;
import org.opensearch.serde.proto.SearchHitsTransportProto.DocumentFieldProto;
import org.opensearch.serde.proto.SearchHitsTransportProto.HighlightFieldProto;
import org.opensearch.serde.proto.SearchHitsTransportProto.SearchSortValuesProto;
import org.opensearch.serde.proto.SearchHitsTransportProto.SearchShardTargetProto;
import org.opensearch.serde.proto.SearchHitsTransportProto.ExplanationProto;
import org.opensearch.serde.proto.SearchHitsTransportProto.ShardIdProto;
import org.opensearch.serde.proto.SearchHitsTransportProto.IndexProto;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableMap;
import static org.opensearch.common.lucene.Lucene.readExplanation;
import static org.opensearch.common.lucene.Lucene.writeExplanation;
import static org.opensearch.search.SearchHit.SINGLE_MAPPING_TYPE;

/**
 * Serialization/Deserialization implementations for SearchHit.
 * @opensearch.internal
 */
public class SearchHitSerDe implements SerDe.StreamSerializer<SearchHit>, SerDe.StreamDeserializer<SearchHit> {
    SearchHitsSerDe searchHitsSerDe = new SearchHitsSerDe();

    @Override
    public SearchHit deserialize(StreamInput in) {
        try {
            return fromStream(in);
        } catch (IOException e) {
            throw new SerDe.SerializationException("Failed to deserialize FetchSearchResult", e);
        }
    }

    @Override
    public void serialize(SearchHit object, StreamOutput out) throws SerDe.SerializationException {
        try {
            toStream(object, out);
        } catch (IOException e) {
            throw new SerDe.SerializationException("Failed to serialize FetchSearchResult", e);
        }
    }

    SearchHitProto toProto(SearchHit searchHit) {
        SearchHit.SerializationAccess serI = searchHit.getSerAccess();

        SearchHitProto.Builder builder = SearchHitProto.newBuilder()
            .setScore(serI.getScore())
            .setId(serI.getId().string())
            .setVersion(serI.getVersion())
            .setSeqNo(serI.getSeqNo())
            .setPrimaryTerm(serI.getPrimaryTerm());

        if (serI.getNestedIdentity() != null) {
            builder.setNestedIdentity(nestedIdentityToProto(serI.getNestedIdentity()));
        }

        if (serI.getSource() != null) {
            builder.setSource(ByteString.copyFrom(serI.getSource().toBytesRef().bytes));
        }

        if (serI.getExplanation() != null) {
            builder.setExplanation(explanationToProto(serI.getExplanation()));
        }

        serI.getDocumentFields().forEach((key, value) ->
            builder.putDocumentFields(key, documentFieldToProto(value))
        );

        serI.getMetaFields().forEach((key, value) ->
            builder.putMetaFields(key, documentFieldToProto(value))
        );

        serI.getHighlightedFields().forEach((key, value) ->
            builder.putHighlightFields(key, highlightFieldToProto(value))
        );

        if (serI.getSortValues() != null) {
            builder.setSortValues(searchSortValuesToProto(serI.getSortValues()));
        }

        serI.getMatchedQueries().forEach(builder::putMatchedQueries);

        if (serI.getShard() != null) {
            builder.setShard(searchShardTargetToProto(serI.getShard()));
        }

        if (serI.getInnerHits() != null) {
            serI.getInnerHits().forEach((key, value) ->
                builder.putInnerHits(key, searchHitsSerDe.toProto(value))
            );
        }

        return builder.build();
    }

    SearchHit fromProto(SearchHitProto proto) throws SerDe.SerializationException {
        int docId = -1;
        float score = proto.getScore();
        long seqNo = proto.getSeqNo();
        long version = proto.getVersion();
        long primaryTerm = proto.getPrimaryTerm();
        Text id = new Text(proto.getId());
        BytesReference source = BytesReference.fromByteBuffer(proto.getSource().asReadOnlyByteBuffer());
        SearchShardTarget shard = searchShardTargetFromProto(proto.getShard());
        Explanation explanation = explanationFromProto(proto.getExplanation());
        SearchSortValues sortValues = searchSortValuesFromProto(proto.getSortValues());
        SearchHit.NestedIdentity nestedIdentity = nestedIdentityFromProto(proto.getNestedIdentity());

        HashMap<String, DocumentField> documentFields = new HashMap<>();
        proto.getDocumentFieldsMap().forEach((key, value) ->
            documentFields.put(key, documentFieldFromProto(value))
        );

        HashMap<String, DocumentField> metaFields = new HashMap<>();
        proto.getMetaFieldsMap().forEach((key, value) ->
            metaFields.put(key, documentFieldFromProto(value))
        );

        HashMap<String, HighlightField> highlightFields = new HashMap<>();
        proto.getHighlightFieldsMap().forEach((key, value) ->
            highlightFields.put(key, highlightFieldFromProto(value))
        );

        Map<String, Float> matchedQueries = proto.getMatchedQueriesMap();

        HashMap<String, SearchHits> innerHits = new HashMap<>();
        proto.getInnerHitsMap().forEach((key, value) ->
            innerHits.put(key, this.searchHitsSerDe.fromProto(value))
        );

        String index = shard.getIndex();
        String clusterAlias = shard.getClusterAlias();

        return new SearchHit(
            docId,
            score,
            seqNo,
            version,
            primaryTerm,
            id,
            source,
            shard,
            explanation,
            sortValues,
            nestedIdentity,
            documentFields,
            metaFields,
            highlightFields,
            matchedQueries,
            innerHits,
            index,
            clusterAlias
        );
    }

    static NestedIdentityProto nestedIdentityToProto(SearchHit.NestedIdentity nestedIdentity) {
        NestedIdentityProto.Builder builder = NestedIdentityProto.newBuilder()
            .setField(nestedIdentity.getField().string())
            .setOffset(nestedIdentity.getOffset());

        if (nestedIdentity.getChild() != null) {
            builder.setChild(nestedIdentityToProto(nestedIdentity.getChild()));
        }

        return builder.build();
    }

    static SearchHit.NestedIdentity nestedIdentityFromProto(NestedIdentityProto proto) {
        String field = proto.getField();
        int offset = proto.getOffset();

        SearchHit.NestedIdentity child = null;
        if (proto.hasChild()) {
            child = nestedIdentityFromProto(proto.getChild());
        }

        return new SearchHit.NestedIdentity(field, offset, child);
    }

    // TODO: Lucene definitions should maybe be serialized as generic bytes arrays.
    static ExplanationProto explanationToProto(Explanation explanation) {
        ExplanationProto.Builder builder = ExplanationProto.newBuilder()
            .setMatch(explanation.isMatch())
            .setValue(explanation.getValue().longValue())
            .setDescription(explanation.getDescription());

        for (Explanation detail : explanation.getDetails()) {
            builder.addDetails(explanationToProto(detail));
        }

        return builder.build();
    }

    static Explanation explanationFromProto(ExplanationProto proto) {
        long value = proto.getValue();
        String description = proto.getDescription();
        Collection<Explanation> details = new ArrayList<>();

        for (ExplanationProto det : proto.getDetailsList()) {
            details.add(explanationFromProto(det));
        }

        if (proto.getMatch()) {
            return Explanation.match(value, description, details);
        }

        return Explanation.noMatch(description, details);
    }

    // Is there a reason to open a new stream for each object?
    // Seems simpler to write a single stream.
    static DocumentFieldProto documentFieldToProto(DocumentField field) {
        DocumentFieldProto.Builder builder = DocumentFieldProto.newBuilder().setName(field.getName());

        try (BytesStreamOutput docsOut = new BytesStreamOutput()) {
            docsOut.writeCollection(field.getValues(), StreamOutput::writeGenericValue);
            builder.addValues(ByteString.copyFrom(docsOut.bytes().toBytesRef().bytes));
        } catch (IOException e){
            builder.addValues(ByteString.EMPTY);
        }

        return builder.build();
    }

    static DocumentField documentFieldFromProto(DocumentFieldProto proto) throws SerDe.SerializationException {
        String name = proto.getName();
        List<Object> values = new ArrayList<>(0);

        if (proto.getValuesCount() > 0) {
            BytesReference valuesBytes = new BytesArray(proto.getValues(0).toByteArray());
            try (StreamInput in = valuesBytes.streamInput()) {
                Object readValue = in.readGenericValue();
                values.add(readValue);
            } catch (IOException e) {
                throw new SerDe.SerializationException("Failed to deserialize DocumentField values from proto object", e);
            }
        }

        return new DocumentField(name, values);
    }

    static HighlightFieldProto highlightFieldToProto(HighlightField field) {
        HighlightFieldProto.Builder builder = HighlightFieldProto.newBuilder()
            .setName(field.getName());

        for (Text frag : field.getFragments()) {
            builder.addFragments(frag.string());
        }

        return builder.build();
    }

    static HighlightField highlightFieldFromProto(HighlightFieldProto proto) {
        String name = proto.getName();
        Text[] fragments = new Text[proto.getFragmentsCount()];

        for (int i = 0; i < proto.getFragmentsCount(); i++) {
            fragments[i] = new Text(proto.getFragments(i));
        }

        return new HighlightField(name, fragments);
    }

    // See above comment for documentFieldToProto.
    static SearchSortValuesProto searchSortValuesToProto(SearchSortValues searchSortValues) {
        SearchSortValuesProto.Builder builder = SearchSortValuesProto.newBuilder();

        try (BytesStreamOutput formOut = new BytesStreamOutput()) {
            formOut.writeArray(Lucene::writeSortValue, searchSortValues.getFormattedSortValues());
            builder.addFormattedSortValues(ByteString.copyFrom(formOut.bytes().toBytesRef().bytes));
        } catch (IOException e){
            builder.addFormattedSortValues(ByteString.EMPTY);
        }

        try (BytesStreamOutput rawOut = new BytesStreamOutput()) {
            rawOut.writeArray(Lucene::writeSortValue, searchSortValues.getFormattedSortValues());
            builder.addRawSortValues(ByteString.copyFrom(rawOut.bytes().toBytesRef().bytes));
        } catch (IOException e){
            builder.addRawSortValues(ByteString.EMPTY);
        }

        return builder.build();
    }

    static SearchSortValues searchSortValuesFromProto(SearchSortValuesProto proto) throws SerDe.SerializationException {
        Object[] formattedSortValues = null;
        Object[] rawSortValues = null;

        if (proto.getFormattedSortValuesCount() > 0) {
            BytesReference formattedBytes = new BytesArray(proto.getFormattedSortValues(0).toByteArray());
            try (StreamInput formattedIn = formattedBytes.streamInput()) {
                formattedSortValues = formattedIn.readArray(Lucene::readSortValue, Object[]::new);
            } catch (IOException e) {
                throw new SerDe.SerializationException("Failed to deserialize SearchSortValues from proto object", e);
            }
        }

        if (proto.getRawSortValuesCount() > 0) {
            BytesReference rawBytes = new BytesArray(proto.getRawSortValues(0).toByteArray());
            try (StreamInput rawIn = rawBytes.streamInput()) {
                rawSortValues = rawIn.readArray(Lucene::readSortValue, Object[]::new);
            } catch (IOException e) {
                throw new SerDe.SerializationException("Failed to deserialize SearchSortValues from proto object", e);
            }
        }

        return new SearchSortValues(formattedSortValues, rawSortValues);
    }

    static SearchShardTargetProto searchShardTargetToProto(SearchShardTarget shardTarget) {
        return SearchShardTargetProto.newBuilder()
            .setNodeId(shardTarget.getNodeId())
            .setShardId(shardIdToProto(shardTarget.getShardId()))
            .setClusterAlias(shardTarget.getClusterAlias())
            .build();
    }

    public static SearchShardTarget searchShardTargetFromProto(SearchShardTargetProto proto) {
        String nodeId = proto.getNodeId();
        ShardId shardId = shardIdFromProto(proto.getShardId());
        String clusterAlias = proto.getClusterAlias();
        return new SearchShardTarget(nodeId, shardId, clusterAlias);
    }

    static ShardIdProto shardIdToProto(ShardId shardId) {
        return ShardIdProto.newBuilder()
            .setIndex(indexToProto(shardId.getIndex()))
            .setShardId(shardId.id())
            .setHashCode(shardId.hashCode())
            .build();
    }

    public static ShardId shardIdFromProto(ShardIdProto proto) {
        Index index = indexFromProto(proto.getIndex());
        int shardId = proto.getShardId();
        return new ShardId(index, shardId);
    }

    static IndexProto indexToProto(Index index) {
        return IndexProto.newBuilder()
            .setName(index.getName())
            .setUuid(index.getUUID())
            .build();
    }

    public static Index indexFromProto(IndexProto proto) {
        String name = proto.getName();
        String uuid = proto.getUuid();
        return new Index(name, uuid);
    }

    private SearchHit fromStream(StreamInput in) throws IOException {
        int docId;
        float score;
        long seqNo;
        long version;
        long primaryTerm;
        Text id;
        BytesReference source;
        SearchShardTarget shard;
        Explanation explanation = null;
        SearchSortValues sortValues;
        SearchHit.NestedIdentity nestedIdentity;
        Map<String, DocumentField> documentFields;
        Map<String, DocumentField> metaFields;
        Map<String, HighlightField> highlightFields;
        Map<String, Float> matchedQueries = Map.of();
        Map<String, SearchHits> innerHits;
        String index = null;
        String clusterAlias = null;

        docId = -1;
        score = in.readFloat();
        id = in.readOptionalText();
        if (in.getVersion().before(Version.V_2_0_0)) {
            in.readOptionalText();
        }
        nestedIdentity = in.readOptionalWriteable(SearchHit.NestedIdentity::new);
        version = in.readLong();
        seqNo = in.readZLong();
        primaryTerm = in.readVLong();
        source = in.readBytesReference();
        if (source.length() == 0) {
            source = null;
        }
        if (in.readBoolean()) {
            explanation = readExplanation(in);
        }
        documentFields = in.readMap(StreamInput::readString, DocumentField::new);
        metaFields = in.readMap(StreamInput::readString, DocumentField::new);

        int size = in.readVInt();
        if (size == 0) {
            highlightFields = emptyMap();
        } else if (size == 1) {
            HighlightField field = new HighlightField(in);
            highlightFields = singletonMap(field.name(), field);
        } else {
            Map<String, HighlightField> hlFields = new HashMap<>();
            for (int i = 0; i < size; i++) {
                HighlightField field = new HighlightField(in);
                hlFields.put(field.name(), field);
            }
            highlightFields = unmodifiableMap(hlFields);
        }

        sortValues = new SearchSortValues(in);

        size = in.readVInt();
        if (in.getVersion().onOrAfter(Version.V_2_13_0)) {
            if (size > 0) {
                Map<String, Float> tempMap = in.readMap(StreamInput::readString, StreamInput::readFloat);
                matchedQueries = tempMap.entrySet()
                    .stream()
                    .sorted(Map.Entry.comparingByKey())
                    .collect(
                        Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (oldValue, newValue) -> oldValue, LinkedHashMap::new)
                    );
            }
        } else {
            matchedQueries = new LinkedHashMap<>(size);
            for (int i = 0; i < size; i++) {
                matchedQueries.put(in.readString(), Float.NaN);
            }
        }
        shard = in.readOptionalWriteable(SearchShardTarget::new);
        if (shard != null) {
            index = shard.getIndex();
            clusterAlias = shard.getClusterAlias();
        }

        size = in.readVInt();
        if (size > 0) {
            innerHits = new HashMap<>(size);
            for (int i = 0; i < size; i++) {
                String key = in.readString();
                SearchHits value = new SearchHits(in);
                innerHits.put(key, value);
            }
        } else {
            innerHits = null;
        }

        return new SearchHit(
            docId,
            score,
            seqNo,
            version,
            primaryTerm,
            id,
            source,
            shard,
            explanation,
            sortValues,
            nestedIdentity,
            documentFields,
            metaFields,
            highlightFields,
            matchedQueries,
            innerHits,
            index,
            clusterAlias
        );
    }

    private void toStream(SearchHit object, StreamOutput out) throws IOException {
        SearchHit.SerializationAccess serI = object.getSerAccess();
        float score = serI.getScore();
        long seqNo = serI.getSeqNo();
        long version = serI.getVersion();
        long primaryTerm = serI.getPrimaryTerm();
        Text id = serI.getId();
        BytesReference source = serI.getSource();
        SearchShardTarget shard = serI.getShard();
        Explanation explanation = serI.getExplanation();
        SearchSortValues sortValues = serI.getSortValues();
        SearchHit.NestedIdentity nestedIdentity = serI.getNestedIdentity();
        Map<String, DocumentField> documentFields = serI.getDocumentFields();
        Map<String, DocumentField> metaFields = serI.getMetaFields();
        Map<String, HighlightField> highlightFields = serI.getHighlightedFields();
        Map<String, Float> matchedQueries = serI.getMatchedQueries();
        Map<String, SearchHits> innerHits = serI.getInnerHits();

        out.writeFloat(score);
        out.writeOptionalText(id);
        if (out.getVersion().before(Version.V_2_0_0)) {
            out.writeOptionalText(SINGLE_MAPPING_TYPE);
        }
        out.writeOptionalWriteable(nestedIdentity);
        out.writeLong(version);
        out.writeZLong(seqNo);
        out.writeVLong(primaryTerm);
        out.writeBytesReference(source);
        if (explanation == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            writeExplanation(out, explanation);
        }
        out.writeMap(documentFields, StreamOutput::writeString, (stream, documentField) -> documentField.writeTo(stream));
        out.writeMap(metaFields, StreamOutput::writeString, (stream, documentField) -> documentField.writeTo(stream));
        if (highlightFields == null) {
            out.writeVInt(0);
        } else {
            out.writeVInt(highlightFields.size());
            for (HighlightField highlightField : highlightFields.values()) {
                highlightField.writeTo(out);
            }
        }
        sortValues.writeTo(out);

        out.writeVInt(matchedQueries.size());
        if (out.getVersion().onOrAfter(Version.V_2_13_0)) {
            if (!matchedQueries.isEmpty()) {
                out.writeMap(matchedQueries, StreamOutput::writeString, StreamOutput::writeFloat);
            }
        } else {
            for (String matchedFilter : matchedQueries.keySet()) {
                out.writeString(matchedFilter);
            }
        }
        out.writeOptionalWriteable(shard);
        if (innerHits == null) {
            out.writeVInt(0);
        } else {
            out.writeVInt(innerHits.size());
            for (Map.Entry<String, SearchHits> entry : innerHits.entrySet()) {
                out.writeString(entry.getKey());
                entry.getValue().writeTo(out);
            }
        }
    }
}
