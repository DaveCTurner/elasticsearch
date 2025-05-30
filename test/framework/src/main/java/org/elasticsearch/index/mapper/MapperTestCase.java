/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.UsageTrackingQueryCachingPolicy;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.LuceneSyntheticSourceChangesSnapshot;
import org.elasticsearch.index.fielddata.FieldDataContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;
import org.elasticsearch.index.fieldvisitor.StoredFieldLoader;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.termvectors.TermVectorsService;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptFactory;
import org.elasticsearch.script.field.DocValuesScriptFieldFactory;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.lookup.LeafStoredFieldsLookup;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.search.lookup.SourceFilter;
import org.elasticsearch.search.lookup.SourceProvider;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Base class for testing {@link Mapper}s.
 */
public abstract class MapperTestCase extends MapperServiceTestCase {

    protected abstract void minimalMapping(XContentBuilder b) throws IOException;

    /**
     * Provides a way for subclasses to define minimal mappings for previous index versions
     * @param b content builder to use for building the minimal mapping
     * @param indexVersion index version the mapping should be created for
     * @throws IOException
     */
    protected void minimalMapping(XContentBuilder b, IndexVersion indexVersion) throws IOException {
        minimalMapping(b);
    }

    /**
     * Writes the field and a sample value for it to the provided {@link XContentBuilder}.
     * To be overridden in case the field should not be written at all in documents,
     * like in the case of runtime fields.
     */
    protected void writeField(XContentBuilder builder) throws IOException {
        builder.field("field");
        builder.value(getSampleValueForDocument());
    }

    /**
     * Returns a sample value for the field, to be used in a document
     */
    protected abstract Object getSampleValueForDocument();

    /**
     * Returns a sample object for the field or exception if the field does not support parsing objects
     */
    protected Object getSampleObjectForDocument() {
        throw new UnsupportedOperationException("Field doesn't support object parsing.");
    }

    /**
     * Returns a sample value for the field, to be used when querying the field. Normally this is the same format as
     * what is indexed as part of a document, and returned by {@link #getSampleValueForDocument()}, but there
     * are cases where fields are queried differently frow how they are indexed e.g. token_count or runtime fields
     */
    protected Object getSampleValueForQuery() {
        return getSampleValueForDocument();
    }

    /**
     * This test verifies that the exists query created is the appropriate one, and aligns with the data structures
     * being created for a document with a value for the field. This can only be verified for the minimal mapping.
     * Field types that allow configurable doc_values or norms should write their own tests that creates the different
     * mappings combinations and invoke {@link #assertExistsQuery(MapperService)} to verify the behaviour.
     */
    public final void testExistsQueryMinimalMapping() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(this::minimalMapping));
        assertExistsQuery(mapperService);
        assertParseMinimalWarnings();
    }

    // TODO make this final once we've worked out what is happening with DenseVector
    public void testAggregatableConsistency() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(this::minimalMapping));
        assertAggregatableConsistency(mapperService.fieldType("field"));
        assertParseMinimalWarnings();
    }

    protected void assertAggregatableConsistency(MappedFieldType ft) {
        if (ft.isAggregatable()) {
            try {
                ft.fielddataBuilder(FieldDataContext.noRuntimeFields("aggregation_test"));
            } catch (Exception e) {
                fail("Unexpected exception when fetching field data from aggregatable field type");
            }
        } else {
            expectThrows(IllegalArgumentException.class, () -> ft.fielddataBuilder(FieldDataContext.noRuntimeFields("aggregation_test")));
        }
    }

    /**
     * Does this field mapper support {@code ignore_malformed}?
     */
    protected abstract boolean supportsIgnoreMalformed();

    /**
     * Build an {@link ExampleMalformedValue} that parses a string.
     */
    protected final ExampleMalformedValue exampleMalformedValue(String value) {
        return exampleMalformedValue(b -> b.value(value));
    }

    /**
     * Build an {@link ExampleMalformedValue} for arbitrary xcontent.
     */
    protected final ExampleMalformedValue exampleMalformedValue(CheckedConsumer<XContentBuilder, IOException> value) {
        return new ExampleMalformedValue(this::minimalMapping, value, equalTo("unset"));
    }

    /**
     * An example of a malformed value.
     */
    public static class ExampleMalformedValue {
        private final CheckedConsumer<XContentBuilder, IOException> mapping;
        private final CheckedConsumer<XContentBuilder, IOException> value;
        private final Matcher<String> exceptionMessageMatcher;

        private ExampleMalformedValue(
            CheckedConsumer<XContentBuilder, IOException> mapping,
            CheckedConsumer<XContentBuilder, IOException> value,
            Matcher<String> exceptionMessageMatcher
        ) {
            this.mapping = mapping;
            this.value = value;
            this.exceptionMessageMatcher = exceptionMessageMatcher;
        }

        /**
         * Set the mapping used for this value. If not called the default is
         * {@link MapperTestCase#minimalMapping}.
         */
        public ExampleMalformedValue mapping(CheckedConsumer<XContentBuilder, IOException> newMapping) {
            return new ExampleMalformedValue(newMapping, value, exceptionMessageMatcher);
        }

        /**
         * Match error messages that contain a string.
         */
        public ExampleMalformedValue errorMatches(String contains) {
            return errorMatches(containsString(contains));
        }

        /**
         * Match the error message in an arbitrary way.
         */
        public ExampleMalformedValue errorMatches(Matcher<String> newMatcher) {
            return new ExampleMalformedValue(mapping, value, newMatcher);
        }
    }

    /**
     * Some example of malformed values and matches for exceptions that parsing them should create.
     */
    protected List<ExampleMalformedValue> exampleMalformedValues() {
        assertFalse("mappers that support ignore_malformed values most override exampleMalformedValues", supportsIgnoreMalformed());
        return List.of();
    }

    public final void testIgnoreMalformedFalseByDefault() throws IOException {
        for (ExampleMalformedValue example : exampleMalformedValues()) {
            assertIgnoreMalformedFalse(example.mapping, example.value, example.exceptionMessageMatcher);
        }
    }

    public final void testIgnoreMalformedExplicitlyFalse() throws IOException {
        if (false == supportsIgnoreMalformed()) {
            Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
                minimalMapping(b);
                b.field("ignore_malformed", false);
            })));
            assertThat(e.getMessage(), containsString("unknown parameter [ignore_malformed] on mapper [field]"));
            return;
        }
        for (ExampleMalformedValue example : exampleMalformedValues()) {
            assertIgnoreMalformedFalse(b -> {
                example.mapping.accept(b);
                b.field("ignore_malformed", false);
            }, example.value, example.exceptionMessageMatcher);
        }
    }

    private void assertIgnoreMalformedFalse(
        CheckedConsumer<XContentBuilder, IOException> mapping,
        CheckedConsumer<XContentBuilder, IOException> value,
        Matcher<String> exceptionMessageMatcher
    ) throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(mapping));
        FieldMapper mapper = (FieldMapper) mapperService.documentMapper().mappers().getMapper("field");
        assertFalse(mapper.ignoreMalformed());
        SourceToParse source = source(b -> {
            b.field("field");
            value.accept(b);
        });
        DocumentParsingException e = expectThrows(
            DocumentParsingException.class,
            "didn't throw while parsing " + source.source().utf8ToString(),
            () -> mapperService.documentMapper().parse(source)
        );
        assertThat(
            "incorrect exception while parsing " + source.source().utf8ToString(),
            e.getCause().getMessage(),
            exceptionMessageMatcher
        );
    }

    public final void testIgnoreMalformedTrue() throws IOException {
        if (false == supportsIgnoreMalformed()) {
            Exception e = expectThrows(MapperParsingException.class, () -> createMapperService(fieldMapping(b -> {
                minimalMapping(b);
                b.field("ignore_malformed", true);
            })));
            assertThat(e.getMessage(), containsString("unknown parameter [ignore_malformed] on mapper [field]"));
            return;
        }
        for (ExampleMalformedValue example : exampleMalformedValues()) {
            XContentBuilder mapping = fieldMapping(b -> {
                example.mapping.accept(b);
                b.field("ignore_malformed", true);
            });
            MapperService mapperService = createMapperService(mapping);
            FieldMapper mapper = (FieldMapper) mapperService.documentMapper().mappers().getMapper("field");
            assertTrue(mapper.ignoreMalformed());
            ParsedDocument doc = mapperService.documentMapper().parse(source(b -> {
                b.field("field");
                example.value.accept(b);
            }));
            List<IndexableField> fields = doc.rootDoc().getFields("field");
            assertThat(fields, empty());
            assertThat(TermVectorsService.getValues(doc.rootDoc().getFields("_ignored")), contains("field"));
        }
    }

    protected void assertExistsQuery(MapperService mapperService) throws IOException {
        LuceneDocument fields = mapperService.documentMapper().parse(source(this::writeField)).rootDoc();
        SearchExecutionContext searchExecutionContext = createSearchExecutionContext(mapperService);
        MappedFieldType fieldType = mapperService.fieldType("field");
        Query query = fieldType.existsQuery(searchExecutionContext);
        assertExistsQuery(fieldType, query, fields);
    }

    protected void assertExistsQuery(MappedFieldType fieldType, Query query, LuceneDocument fields) {
        if (fieldType.hasDocValues() || fieldType.getTextSearchInfo().hasNorms()) {
            assertThat(query, instanceOf(FieldExistsQuery.class));
            FieldExistsQuery fieldExistsQuery = (FieldExistsQuery) query;
            assertEquals("field", fieldExistsQuery.getField());
            assertNoFieldNamesField(fields);
        } else {
            assertThat(query, instanceOf(TermQuery.class));
            TermQuery termQuery = (TermQuery) query;
            assertEquals(FieldNamesFieldMapper.NAME, termQuery.getTerm().field());
            // we always perform a term query against _field_names, even when the field
            // is not added to _field_names because it is not indexed nor stored
            assertEquals("field", termQuery.getTerm().text());
            assertNoDocValuesField(fields, "field");
            if (fieldType.isIndexed() || fieldType.isStored()) {
                assertNotNull(fields.getField(FieldNamesFieldMapper.NAME));
            } else {
                assertNoFieldNamesField(fields);
            }
        }
    }

    protected static void assertNoFieldNamesField(LuceneDocument fields) {
        assertNull(fields.getField(FieldNamesFieldMapper.NAME));
    }

    protected static void assertHasNorms(LuceneDocument doc, String field) {
        List<IndexableField> fields = doc.getFields(field);
        for (IndexableField indexableField : fields) {
            IndexableFieldType indexableFieldType = indexableField.fieldType();
            if (indexableFieldType.indexOptions() != IndexOptions.NONE) {
                assertFalse(indexableFieldType.omitNorms());
                return;
            }
        }
        fail("field [" + field + "] should be indexed but it isn't");
    }

    protected static void assertNoDocValuesField(LuceneDocument doc, String field) {
        List<IndexableField> fields = doc.getFields(field);
        for (IndexableField indexableField : fields) {
            assertEquals(DocValuesType.NONE, indexableField.fieldType().docValuesType());
        }
    }

    protected <T> void assertDimension(boolean isDimension, Function<T, Boolean> checker) throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("time_series_dimension", isDimension);
        }));

        @SuppressWarnings("unchecked") // Syntactic sugar in tests
        T fieldType = (T) mapperService.fieldType("field");
        assertThat(checker.apply(fieldType), equalTo(isDimension));
    }

    protected <T> void assertMetricType(String metricType, Function<T, Enum<TimeSeriesParams.MetricType>> checker) throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> {
            minimalMapping(b);
            b.field("time_series_metric", metricType);
        }));

        @SuppressWarnings("unchecked") // Syntactic sugar in tests
        T fieldType = (T) mapperService.fieldType("field");
        assertThat(checker.apply(fieldType).toString(), equalTo(metricType));
    }

    public final void testEmptyName() {
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(mapping(b -> {
            b.startObject("");
            minimalMapping(b);
            b.endObject();
        })));
        assertThat(e.getMessage(), containsString("field name cannot be an empty string"));
    }

    public final void testBlankName() {
        IndexVersion version = getVersion();
        assumeTrue("blank field names are rejected from 8.6.0 onwards", version.onOrAfter(IndexVersions.V_8_6_0));
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(version, mapping(b -> {
            b.startObject("  ");
            minimalMapping(b);
            b.endObject();
        })));
        assertThat(e.getMessage(), containsString("field name cannot contain only whitespaces"));
    }

    public final void testMinimalSerializesToItself() throws IOException {
        XContentBuilder orig = JsonXContent.contentBuilder().startObject();
        createMapperService(fieldMapping(this::minimalMapping)).documentMapper().mapping().toXContent(orig, ToXContent.EMPTY_PARAMS);
        orig.endObject();
        XContentBuilder parsedFromOrig = JsonXContent.contentBuilder().startObject();
        createMapperService(orig).documentMapper().mapping().toXContent(parsedFromOrig, ToXContent.EMPTY_PARAMS);
        parsedFromOrig.endObject();
        assertEquals(Strings.toString(orig), Strings.toString(parsedFromOrig));
        assertParseMinimalWarnings();
    }

    public final void testMinimalToMaximal() throws IOException {
        XContentBuilder orig = JsonXContent.contentBuilder().startObject();
        createMapperService(fieldMapping(this::minimalMapping)).documentMapper().mapping().toXContent(orig, INCLUDE_DEFAULTS);
        orig.endObject();
        XContentBuilder parsedFromOrig = JsonXContent.contentBuilder().startObject();
        createMapperService(orig).documentMapper().mapping().toXContent(parsedFromOrig, INCLUDE_DEFAULTS);
        parsedFromOrig.endObject();
        assertEquals(Strings.toString(orig), Strings.toString(parsedFromOrig));
        assertParseMaximalWarnings();
    }

    public void testTotalFieldsCount() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(this::minimalMapping));
        assertEquals(1, mapperService.documentMapper().mapping().getRoot().getTotalFieldsCount());
    }

    protected final void assertParseMinimalWarnings() {
        String[] warnings = getParseMinimalWarnings();
        if (warnings.length > 0) {
            assertWarnings(warnings);
        }
    }

    protected final void assertParseMaximalWarnings() {
        String[] warnings = getParseMaximalWarnings();
        if (warnings.length > 0) {
            assertWarnings(warnings);
        }
    }

    protected String[] getParseMinimalWarnings() {
        // Most mappers don't emit any warnings
        return Strings.EMPTY_ARRAY;
    }

    protected String[] getParseMinimalWarnings(IndexVersion indexVersion) {
        return getParseMinimalWarnings();
    }

    protected String[] getParseMaximalWarnings() {
        // Most mappers don't emit any warnings
        return Strings.EMPTY_ARRAY;
    }

    /**
     * Override to disable testing {@code meta} in fields that don't support it.
     */
    protected boolean supportsMeta() {
        return true;
    }

    /**
     * Override to disable testing {@code copy_to} in fields that don't support it.
     */
    protected boolean supportsCopyTo() {
        return true;
    }

    protected void metaMapping(XContentBuilder b) throws IOException {
        minimalMapping(b);
    }

    public final void testMeta() throws IOException {
        assumeTrue("Field doesn't support meta", supportsMeta());
        XContentBuilder mapping = fieldMapping(b -> {
            metaMapping(b);
            b.field("meta", Collections.singletonMap("foo", "bar"));
        });
        MapperService mapperService = createMapperService(mapping);
        assertEquals(
            XContentHelper.convertToMap(BytesReference.bytes(mapping), false, mapping.contentType()).v2(),
            XContentHelper.convertToMap(mapperService.documentMapper().mappingSource().uncompressed(), false, mapping.contentType()).v2()
        );

        mapping = fieldMapping(this::metaMapping);
        merge(mapperService, mapping);
        assertEquals(
            XContentHelper.convertToMap(BytesReference.bytes(mapping), false, mapping.contentType()).v2(),
            XContentHelper.convertToMap(mapperService.documentMapper().mappingSource().uncompressed(), false, mapping.contentType()).v2()
        );

        mapping = fieldMapping(b -> {
            metaMapping(b);
            b.field("meta", Collections.singletonMap("baz", "quux"));
        });
        merge(mapperService, mapping);
        assertEquals(
            XContentHelper.convertToMap(BytesReference.bytes(mapping), false, mapping.contentType()).v2(),
            XContentHelper.convertToMap(mapperService.documentMapper().mappingSource().uncompressed(), false, mapping.contentType()).v2()
        );
    }

    public void testBoostNotAllowed() throws IOException {
        MapperParsingException e = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(boostNotAllowedIndexVersion(), fieldMapping(b -> {
                minimalMapping(b);
                b.field("boost", 2.0);
            }))
        );
        assertThat(e.getMessage(), anyOf(containsString("Unknown parameter [boost]"), containsString("[boost : 2.0]")));

        assertParseMinimalWarnings();
    }

    protected IndexVersion boostNotAllowedIndexVersion() {
        return IndexVersions.V_8_0_0;
    }

    /**
     * Use a {@linkplain ValueFetcher} to extract values from doc values.
     */
    protected final List<?> fetchFromDocValues(MapperService mapperService, MappedFieldType ft, DocValueFormat format, Object sourceValue)
        throws IOException {

        SetOnce<List<?>> result = new SetOnce<>();
        withLuceneIndex(mapperService, iw -> {
            iw.addDocument(mapperService.documentMapper().parse(source(b -> b.field(ft.name(), sourceValue))).rootDoc());
        }, iw -> {
            SearchLookup lookup = new SearchLookup(
                mapperService::fieldType,
                fieldDataLookup(mapperService),
                SourceProvider.fromLookup(mapperService.mappingLookup(), null, mapperService.getMapperMetrics().sourceFieldMetrics())
            );
            ValueFetcher valueFetcher = new DocValueFetcher(format, lookup.getForField(ft, MappedFieldType.FielddataOperation.SEARCH));
            IndexSearcher searcher = newSearcher(iw);
            LeafReaderContext context = searcher.getIndexReader().leaves().get(0);
            Source source = lookup.getSource(context, 0);
            valueFetcher.setNextReader(context);
            result.set(valueFetcher.fetchValues(source, 0, new ArrayList<>()));
        });
        return result.get();
    }

    protected static void assertScriptDocValues(MapperService mapperService, Object sourceValue, Matcher<List<?>> dvMatcher)
        throws IOException {
        withLuceneIndex(mapperService, iw -> {
            iw.addDocument(mapperService.documentMapper().parse(source(b -> b.field("field", sourceValue))).rootDoc());
        }, iw -> {
            IndexSearcher searcher = newSearcher(iw);
            MappedFieldType ft = mapperService.fieldType("field");
            SourceProvider sourceProvider = mapperService.mappingLookup().isSourceSynthetic() ? (ctx, doc) -> {
                throw new IllegalArgumentException("Can't load source in scripts in synthetic mode");
            } : SourceProvider.fromLookup(mapperService.mappingLookup(), null, mapperService.getMapperMetrics().sourceFieldMetrics());
            SearchLookup searchLookup = new SearchLookup(null, null, sourceProvider);
            IndexFieldData<?> sfd = ft.fielddataBuilder(
                new FieldDataContext(
                    "",
                    mapperService.getIndexSettings(),
                    () -> searchLookup,
                    Set::of,
                    MappedFieldType.FielddataOperation.SCRIPT
                )
            ).build(null, null);
            LeafFieldData lfd = sfd.load(getOnlyLeafReader(searcher.getIndexReader()).getContext());
            DocValuesScriptFieldFactory sff = lfd.getScriptFieldFactory("field");
            sff.setNextDocId(0);
            assertThat(sff.toScriptDocValues(), dvMatcher);
        });
    }

    private class UpdateCheck {
        final XContentBuilder init;
        final XContentBuilder update;
        final Consumer<FieldMapper> check;

        private UpdateCheck(CheckedConsumer<XContentBuilder, IOException> update, Consumer<FieldMapper> check) throws IOException {
            this.init = fieldMapping(MapperTestCase.this::minimalMapping);
            this.update = fieldMapping(b -> {
                minimalMapping(b);
                update.accept(b);
            });
            this.check = check;
        }

        private UpdateCheck(
            CheckedConsumer<XContentBuilder, IOException> init,
            CheckedConsumer<XContentBuilder, IOException> update,
            Consumer<FieldMapper> check
        ) throws IOException {
            this.init = fieldMapping(init);
            this.update = fieldMapping(update);
            this.check = check;
        }
    }

    private record ConflictCheck(XContentBuilder init, XContentBuilder update) {}

    public class ParameterChecker {

        List<UpdateCheck> updateChecks = new ArrayList<>();
        Map<String, ConflictCheck> conflictChecks = new HashMap<>();

        /**
         * Register a check that a parameter can be updated, using the minimal mapping as a base
         *
         * @param update a field builder applied on top of the minimal mapping
         * @param check  a check that the updated parameter has been applied to the FieldMapper
         */
        public void registerUpdateCheck(CheckedConsumer<XContentBuilder, IOException> update, Consumer<FieldMapper> check)
            throws IOException {
            updateChecks.add(new UpdateCheck(update, check));
        }

        /**
         * Register a check that a parameter can be updated
         *
         * @param init   the initial mapping
         * @param update the updated mapping
         * @param check  a check that the updated parameter has been applied to the FieldMapper
         */
        public void registerUpdateCheck(
            CheckedConsumer<XContentBuilder, IOException> init,
            CheckedConsumer<XContentBuilder, IOException> update,
            Consumer<FieldMapper> check
        ) throws IOException {
            updateChecks.add(new UpdateCheck(init, update, check));
        }

        /**
         * Register a check that a parameter update will cause a conflict, using the minimal mapping as a base
         *
         * @param param  the parameter name, expected to appear in the error message
         * @param update a field builder applied on top of the minimal mapping
         */
        public void registerConflictCheck(String param, CheckedConsumer<XContentBuilder, IOException> update) throws IOException {
            conflictChecks.put(param, new ConflictCheck(fieldMapping(MapperTestCase.this::minimalMapping), fieldMapping(b -> {
                minimalMapping(b);
                update.accept(b);
            })));
        }

        /**
         * Register a check that a parameter update will cause a conflict
         *
         * @param param  the parameter name, expected to appear in the error message
         * @param init   the initial mapping
         * @param update the updated mapping
         */
        public void registerConflictCheck(String param, XContentBuilder init, XContentBuilder update) {
            conflictChecks.put(param, new ConflictCheck(init, update));
        }
    }

    protected abstract void registerParameters(ParameterChecker checker) throws IOException;

    public void testUpdates() throws IOException {
        ParameterChecker checker = new ParameterChecker();
        registerParameters(checker);
        if (supportsIgnoreMalformed()) {
            checker.registerUpdateCheck(b -> b.field("ignore_malformed", true), m -> assertTrue(m.ignoreMalformed()));
        } else {
            MapperService mapperService = createMapperService(fieldMapping(this::minimalMapping));
            Exception e = expectThrows(
                MapperParsingException.class,
                "No conflict when setting parameter [ignore_malformed]",
                () -> merge(mapperService, fieldMapping(b -> {
                    minimalMapping(b);
                    b.field("ignore_malformed", true);
                }))
            );
            assertThat(e.getMessage(), containsString("unknown parameter [ignore_malformed] on mapper [field]"));
        }

        for (UpdateCheck updateCheck : checker.updateChecks) {
            MapperService mapperService = createMapperService(updateCheck.init);
            merge(mapperService, updateCheck.update);
            FieldMapper mapper = (FieldMapper) mapperService.documentMapper().mappers().getMapper("field");
            updateCheck.check.accept(mapper);
            // do it again to ensure that we don't get conflicts the second time
            merge(mapperService, updateCheck.update);
            mapper = (FieldMapper) mapperService.documentMapper().mappers().getMapper("field");
            updateCheck.check.accept(mapper);
        }
        for (String param : checker.conflictChecks.keySet()) {
            MapperService mapperService = createMapperService(checker.conflictChecks.get(param).init);
            // merging the same change is fine
            merge(mapperService, checker.conflictChecks.get(param).init);
            // merging the conflicting update should throw an exception
            Exception e = expectThrows(
                IllegalArgumentException.class,
                "No conflict when updating parameter [" + param + "]",
                () -> merge(mapperService, checker.conflictChecks.get(param).update)
            );
            assertThat(
                e.getMessage(),
                anyOf(
                    containsString("Cannot update parameter [" + param + "]"),
                    containsString("different [" + param + "]"),
                    containsString("[" + param + "] cannot be ")
                )
            );
        }
        assertParseMaximalWarnings();
    }

    public final void testTextSearchInfoConsistency() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(this::minimalMapping));
        MappedFieldType fieldType = mapperService.fieldType("field");
        if (fieldType.getTextSearchInfo() == TextSearchInfo.NONE) {
            expectThrows(IllegalArgumentException.class, () -> fieldType.termQuery(null, null));
        } else {
            SearchExecutionContext searchExecutionContext = createSearchExecutionContext(mapperService);
            assertNotNull(fieldType.termQuery(getSampleValueForQuery(), searchExecutionContext));
        }
        assertSearchable(fieldType);
        assertParseMinimalWarnings();
    }

    protected void assertSearchable(MappedFieldType fieldType) {
        assertEquals(fieldType.isIndexed(), fieldType.getTextSearchInfo() != TextSearchInfo.NONE);
    }

    /**
     * Asserts that fetching a single value from doc values and from the native
     * {@link MappedFieldType#valueFetcher} produce the same results.
     * <p>
     * Generally this method covers many many random cases but rarely. So if
     * it fails its generally a good idea to capture its randomized
     * parameters into a new method so we can be sure we consistently test
     * any unique and interesting failure case. See the tests for
     * {@link DateFieldMapper} for some examples.
     */
    public final void testFetch() throws IOException {
        MapperService mapperService = randomFetchTestMapper();
        try {
            MappedFieldType ft = mapperService.fieldType("field");
            assertFetch(mapperService, "field", generateRandomInputValue(ft), randomFetchTestFormat());
        } finally {
            assertParseMinimalWarnings();
        }
    }

    /**
     * Asserts that fetching many values from doc values and from the native
     * {@link MappedFieldType#valueFetcher} produce the same results.
     * <p>
     * Generally this method covers many many random cases but rarely. So if
     * it fails its generally a good idea to capture its randomized
     * parameters into a new method so we can be sure we consistently test
     * any unique and interesting failure case. See the tests for
     * {@link DateFieldMapper} for some examples.
     */
    public final void testFetchMany() throws IOException {
        MapperService mapperService = randomFetchTestMapper();
        try {
            MappedFieldType ft = mapperService.fieldType("field");
            int count = between(2, 10);
            List<Object> values = new ArrayList<>(count);
            while (values.size() < count) {
                values.add(generateRandomInputValue(ft));
            }
            assertFetchMany(mapperService, "field", values, randomFetchTestFormat(), count);
        } finally {
            assertParseMinimalWarnings();
        }
    }

    protected final MapperService randomFetchTestMapper() throws IOException {
        return createMapperService(mapping(b -> {
            b.startObject("field");
            randomFetchTestFieldConfig(b);
            b.endObject();
        }));
    }

    /**
     * Field configuration for {@link #testFetch} and {@link #testFetchMany}.
     * Default implementation delegates to {@link #minimalMapping} but can
     * be overridden to randomize the field type and options.
     */
    protected void randomFetchTestFieldConfig(XContentBuilder b) throws IOException {
        minimalMapping(b);
    }

    /**
     * A random format to use when tripping in {@link #testFetch} and
     * {@link #testFetchMany}.
     */
    protected String randomFetchTestFormat() {
        return null;
    }

    /**
     * Test that dimension parameter is not updateable
     */
    protected void registerDimensionChecks(ParameterChecker checker) throws IOException {
        // dimension cannot be updated
        checker.registerConflictCheck("time_series_dimension", b -> b.field("time_series_dimension", true));
        checker.registerConflictCheck("time_series_dimension", b -> b.field("time_series_dimension", false));
        checker.registerConflictCheck("time_series_dimension", fieldMapping(b -> {
            minimalMapping(b);
            b.field("time_series_dimension", false);
        }), fieldMapping(b -> {
            minimalMapping(b);
            b.field("time_series_dimension", true);
        }));
        checker.registerConflictCheck("time_series_dimension", fieldMapping(b -> {
            minimalMapping(b);
            b.field("time_series_dimension", true);
        }), fieldMapping(b -> {
            minimalMapping(b);
            b.field("time_series_dimension", false);
        }));
    }

    /**
     * Create a random {@code _source} value for this field. Must be compatible
     * with {@link XContentBuilder#value(Object)} and the field's parser.
     */
    protected abstract Object generateRandomInputValue(MappedFieldType ft);

    /**
     * Assert that fetching many values using {@link MappedFieldType#valueFetcher}
     * produces the same values as fetching using doc values.
     */
    protected void assertFetchMany(MapperService mapperService, String field, Object value, String format, int count) throws IOException {
        assertFetch(mapperService, field, value, format);
    }

    /**
     * Assert that fetching a value using {@link MappedFieldType#valueFetcher}
     * produces the same value as fetching using doc values.
     */
    protected void assertFetch(MapperService mapperService, String field, Object value, String format) throws IOException {
        MappedFieldType ft = mapperService.fieldType(field);
        MappedFieldType.FielddataOperation fdt = MappedFieldType.FielddataOperation.SEARCH;
        SourceToParse source = source(b -> b.field(ft.name(), value));
        ValueFetcher docValueFetcher = new DocValueFetcher(
            ft.docValueFormat(format, null),
            ft.fielddataBuilder(FieldDataContext.noRuntimeFields("test"))
                .build(new IndexFieldDataCache.None(), new NoneCircuitBreakerService())
        );
        SearchExecutionContext searchExecutionContext = mock(SearchExecutionContext.class);
        when(searchExecutionContext.isSourceEnabled()).thenReturn(true);
        when(searchExecutionContext.sourcePath(field)).thenReturn(Set.of(field));
        when(searchExecutionContext.getForField(ft, fdt)).thenAnswer(inv -> fieldDataLookup(mapperService).apply(ft, () -> {
            throw new UnsupportedOperationException();
        }, fdt));
        ValueFetcher nativeFetcher = ft.valueFetcher(searchExecutionContext, format);
        ParsedDocument doc = mapperService.documentMapper().parse(source);
        withLuceneIndex(mapperService, iw -> iw.addDocuments(doc.docs()), ir -> {
            Source s = SourceProvider.fromLookup(mapperService.mappingLookup(), null, mapperService.getMapperMetrics().sourceFieldMetrics())
                .getSource(ir.leaves().get(0), 0);
            docValueFetcher.setNextReader(ir.leaves().get(0));
            nativeFetcher.setNextReader(ir.leaves().get(0));
            List<Object> fromDocValues = docValueFetcher.fetchValues(s, 0, new ArrayList<>());
            List<Object> fromNative = nativeFetcher.fetchValues(s, 0, new ArrayList<>());
            /*
             * The native fetcher uses byte, short, etc but doc values always
             * uses long or double. This difference is fine because on the outside
             * users can't see it.
             */
            fromNative = fromNative.stream().map(o -> {
                if (o instanceof Integer || o instanceof Short || o instanceof Byte) {
                    return ((Number) o).longValue();
                }
                if (o instanceof Float) {
                    return ((Float) o).doubleValue();
                }
                return o;
            }).collect(toList());

            if (dedupAfterFetch()) {
                fromNative = fromNative.stream().distinct().collect(Collectors.toList());
            }
            /*
             * Doc values sort according to something appropriate to the field
             * and the native fetchers usually don't sort. We're ok with this
             * difference. But we have to convince the test we're ok with it.
             */
            assertThat("fetching " + value, fromNative, containsInAnyOrder(fromDocValues.toArray()));
        });
    }

    /**
     * A few field types (e.g. keyword fields) don't allow duplicate values, so in those cases we need to de-dup our expected values.
     * Field types where this is the case should overwrite this. The default is to not de-duplicate though.
     */
    protected boolean dedupAfterFetch() {
        return false;
    }

    /**
     * @return whether or not this field type supports access to its values from a SearchLookup
     */
    protected boolean supportsSearchLookup() {
        return true;
    }

    /**
     * Checks that field data from this field produces the same values for query-time
     * scripts and for index-time scripts
     */
    public final void testIndexTimeFieldData() throws IOException {
        assumeTrue("Field type does not support access via search lookup", supportsSearchLookup());
        MapperService mapperService = createMapperService(fieldMapping(this::minimalMapping));
        assertParseMinimalWarnings();
        MappedFieldType fieldType = mapperService.fieldType("field");
        if (fieldType.isAggregatable() == false) {
            return; // No field data available, so we ignore
        }
        SourceToParse source = source(this::writeField);
        ParsedDocument doc = mapperService.documentMapper().parse(source);

        withLuceneIndex(mapperService, iw -> iw.addDocument(doc.rootDoc()), ir -> {

            LeafReaderContext ctx = ir.leaves().get(0);

            DocValuesScriptFieldFactory docValuesFieldSource = fieldType.fielddataBuilder(FieldDataContext.noRuntimeFields("test"))
                .build(new IndexFieldDataCache.None(), new NoneCircuitBreakerService())
                .load(ctx)
                .getScriptFieldFactory("test");
            docValuesFieldSource.setNextDocId(0);

            DocumentLeafReader reader = new DocumentLeafReader(doc.rootDoc(), Collections.emptyMap());
            DocValuesScriptFieldFactory indexData = fieldType.fielddataBuilder(FieldDataContext.noRuntimeFields("test"))
                .build(new IndexFieldDataCache.None(), new NoneCircuitBreakerService())
                .load(reader.getContext())
                .getScriptFieldFactory("test");

            indexData.setNextDocId(0);

            // compare index and search time fielddata
            assertThat(docValuesFieldSource.toScriptDocValues(), equalTo(indexData.toScriptDocValues()));
        });
    }

    protected boolean supportsStoredFields() {
        return true;
    }

    protected void minimalStoreMapping(XContentBuilder b) throws IOException {
        minimalMapping(b);
        b.field("store", true);
    }

    /**
     * Checks that loading stored fields for this field produces the same set of values
     * for query time scripts and index time scripts
     */
    public final void testIndexTimeStoredFieldsAccess() throws IOException {

        assumeTrue("Field type does not support stored fields", supportsStoredFields());
        MapperService mapperService = createMapperService(fieldMapping(this::minimalStoreMapping));
        assertParseMinimalWarnings();

        MappedFieldType fieldType = mapperService.fieldType("field");
        SourceToParse source = source(this::writeField);
        ParsedDocument doc = mapperService.documentMapper().parse(source);

        SearchLookup lookup = new SearchLookup(
            f -> fieldType,
            (f, s, t) -> { throw new UnsupportedOperationException(); },
            (ctx, docid) -> Source.fromBytes(doc.source())
        );

        withLuceneIndex(mapperService, iw -> iw.addDocument(doc.rootDoc()), ir -> {

            LeafReaderContext ctx = ir.leaves().get(0);
            LeafStoredFieldsLookup storedFields = lookup.getLeafSearchLookup(ctx).fields();
            storedFields.setDocument(0);

            DocumentLeafReader reader = new DocumentLeafReader(doc.rootDoc(), Collections.emptyMap());

            LeafStoredFieldsLookup indexStoredFields = lookup.getLeafSearchLookup(reader.getContext()).fields();
            indexStoredFields.setDocument(0);

            // compare index and search time stored fields
            assertThat(storedFields.get("field").getValues(), equalTo(indexStoredFields.get("field").getValues()));
        });
    }

    public final void testNullInput() throws Exception {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        if (allowsNullValues()) {
            ParsedDocument doc = mapper.parse(source(b -> b.nullField("field")));
            assertThat(doc.docs().get(0).getFields("field"), empty());
            assertThat(doc.docs().get(0).getFields("_field_names"), empty());
        } else {
            expectThrows(DocumentParsingException.class, () -> mapper.parse(source(b -> b.nullField("field"))));
        }

        assertWarnings(getParseMinimalWarnings());
    }

    protected boolean allowsNullValues() {
        return true;
    }

    public final void testMinimalIsInvalidInRoutingPath() throws IOException {
        MapperService mapper = createMapperService(fieldMapping(this::minimalMapping));
        try {
            IndexSettings settings = createIndexSettings(
                IndexVersion.current(),
                Settings.builder()
                    .put(IndexSettings.MODE.getKey(), "time_series")
                    .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "field")
                    .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2021-04-28T00:00:00Z")
                    .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2021-04-29T00:00:00Z")
                    .build()
            );
            Exception e = expectThrows(IllegalArgumentException.class, () -> mapper.documentMapper().validate(settings, false));
            assertThat(e.getMessage(), equalTo(minimalIsInvalidRoutingPathErrorMessage(mapper.mappingLookup().getMapper("field"))));
        } finally {
            assertParseMinimalWarnings();
        }
    }

    protected String minimalIsInvalidRoutingPathErrorMessage(Mapper mapper) {
        if (mapper instanceof FieldMapper fieldMapper && fieldMapper.fieldType().isDimension() == false) {
            return "All fields that match routing_path must be configured with [time_series_dimension: true] "
                + "or flattened fields with a list of dimensions in [time_series_dimensions] and "
                + "without the [script] parameter. ["
                + mapper.fullPath()
                + "] was not a dimension.";
        }
        return "All fields that match routing_path must be configured with [time_series_dimension: true] "
            + "or flattened fields with a list of dimensions in [time_series_dimensions] and "
            + "without the [script] parameter. ["
            + mapper.fullPath()
            + "] was ["
            + mapper.typeName()
            + "].";
    }

    public record SyntheticSourceExample(
        CheckedConsumer<XContentBuilder, IOException> inputValue,
        CheckedConsumer<XContentBuilder, IOException> expectedForSyntheticSource,
        CheckedConsumer<XContentBuilder, IOException> mapping
    ) {
        public SyntheticSourceExample(Object inputValue, Object result, CheckedConsumer<XContentBuilder, IOException> mapping) {
            this(b -> b.value(inputValue), b -> b.value(result), mapping);
        }

        public void buildInput(XContentBuilder b) throws IOException {
            b.field("field");
            inputValue.accept(b);
        }

        public void buildInputArray(XContentBuilder b, int elementCount) throws IOException {
            b.startArray("field");
            for (int i = 0; i < elementCount; i++) {
                inputValue.accept(b);
            }
            b.endArray();
        }

        private String expected() throws IOException {
            XContentBuilder b = JsonXContent.contentBuilder().startObject().field("field");
            expectedForSyntheticSource.accept(b);
            return Strings.toString(b.endObject());
        }
    }

    public record SyntheticSourceInvalidExample(Matcher<String> error, CheckedConsumer<XContentBuilder, IOException> mapping) {}

    public interface SyntheticSourceSupport {
        /**
         * @return True if synthetic source support is implemented to exactly store the source
         *         without modifications.
         */
        default boolean preservesExactSource() {
            return false;
        }

        default boolean ignoreAbove() {
            return false;
        }

        /**
         * Examples that should work when source is generated from doc values.
         */
        SyntheticSourceExample example(int maxValues) throws IOException;

        /**
         * Examples of mappings that should be rejected when source is configured to
         * be loaded from doc values.
         */
        List<SyntheticSourceInvalidExample> invalidExample() throws IOException;
    }

    protected abstract SyntheticSourceSupport syntheticSourceSupport(boolean ignoreMalformed);

    protected SyntheticSourceSupport syntheticSourceSupport(boolean ignoreMalformed, boolean columnReader) {
        return syntheticSourceSupport(ignoreMalformed);
    }

    public final void testSyntheticSource() throws IOException {
        assertSyntheticSource(syntheticSourceSupport(shouldUseIgnoreMalformed()).example(5));
    }

    public final void testSyntheticSourceWithTranslogSnapshot() throws IOException {
        assertSyntheticSourceWithTranslogSnapshot(syntheticSourceSupport(shouldUseIgnoreMalformed()), true);
        assertSyntheticSourceWithTranslogSnapshot(syntheticSourceSupport(shouldUseIgnoreMalformed()), false);
    }

    public void testSyntheticSourceIgnoreMalformedExamples() throws IOException {
        assumeTrue("type doesn't support ignore_malformed", supportsIgnoreMalformed());
        // We need to call this in order to hit the assumption inside so that
        // it tells us when field supports ignore_malformed but doesn't support it together with synthetic source.
        // E.g. `assumeFalse(ignoreMalformed)`
        syntheticSourceSupport(true);

        for (ExampleMalformedValue v : exampleMalformedValues()) {
            CheckedConsumer<XContentBuilder, IOException> mapping = b -> {
                v.mapping.accept(b);
                b.field("ignore_malformed", true);
            };
            assertSyntheticSource(new SyntheticSourceExample(v.value, v.value, mapping));
        }
    }

    private void assertSyntheticSource(SyntheticSourceExample example) throws IOException {
        DocumentMapper mapper = createSytheticSourceMapperService(mapping(b -> {
            b.startObject("field");
            example.mapping().accept(b);
            b.endObject();
        })).documentMapper();
        assertThat(syntheticSource(mapper, example::buildInput), equalTo(example.expected()));
        assertThat(
            syntheticSource(mapper, new SourceFilter(new String[] { "field" }, null), example::buildInput),
            equalTo(example.expected())
        );
        assertThat(syntheticSource(mapper, new SourceFilter(null, new String[] { "field" }), example::buildInput), equalTo("{}"));
    }

    private void assertSyntheticSourceWithTranslogSnapshot(SyntheticSourceSupport support, boolean doIndexSort) throws IOException {
        var firstExample = support.example(1);
        int maxDocs = randomIntBetween(20, 50);
        var settings = Settings.builder()
            .put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), SourceFieldMapper.Mode.SYNTHETIC)
            .put(IndexSettings.RECOVERY_USE_SYNTHETIC_SOURCE_SETTING.getKey(), true)
            .build();
        var mapperService = createMapperService(getVersion(), settings, () -> true, mapping(b -> {
            b.startObject("field");
            firstExample.mapping().accept(b);
            b.endObject();
        }));
        var docMapper = mapperService.documentMapper();
        try (var directory = newDirectory()) {
            List<SyntheticSourceExample> examples = new ArrayList<>();
            IndexWriterConfig config = newIndexWriterConfig(random(), new StandardAnalyzer());
            config.setIndexSort(new Sort(new SortField("sort", SortField.Type.LONG)));
            try (var iw = new RandomIndexWriter(random(), directory, config)) {
                for (int seqNo = 0; seqNo < maxDocs; seqNo++) {
                    var example = support.example(randomIntBetween(1, 5));
                    examples.add(example);
                    var doc = docMapper.parse(source(example::buildInput));
                    assertNull(doc.dynamicMappingsUpdate());
                    doc.updateSeqID(seqNo, 1);
                    doc.version().setLongValue(0);
                    if (doIndexSort) {
                        doc.rootDoc().add(new NumericDocValuesField("sort", randomLong()));
                    }
                    iw.addDocuments(doc.docs());
                    if (frequently()) {
                        iw.flush();
                    }
                }
            }
            try (var indexReader = wrapInMockESDirectoryReader(DirectoryReader.open(directory))) {
                int start = randomBoolean() ? 0 : randomIntBetween(1, maxDocs - 10);
                var snapshot = new LuceneSyntheticSourceChangesSnapshot(
                    mapperService,
                    new Engine.Searcher(
                        "recovery",
                        indexReader,
                        new BM25Similarity(),
                        null,
                        new UsageTrackingQueryCachingPolicy(),
                        () -> {}
                    ),
                    randomIntBetween(1, maxDocs),
                    randomLongBetween(0, ByteSizeValue.ofBytes(Integer.MAX_VALUE).getBytes()),
                    start,
                    maxDocs,
                    true,
                    randomBoolean(),
                    IndexVersion.current()
                );
                for (int i = start; i < maxDocs; i++) {
                    var example = examples.get(i);
                    var op = snapshot.next();
                    if (op instanceof Translog.Index opIndex) {
                        assertThat(opIndex.source().utf8ToString(), equalTo(example.expected()));
                    }
                }
            }
        }
    }

    protected boolean supportsEmptyInputArray() {
        return true;
    }

    public void testSupportsParsingObject() throws IOException {
        DocumentMapper mapper = createMapperService(fieldMapping(this::minimalMapping)).documentMapper();
        FieldMapper fieldMapper = (FieldMapper) mapper.mappers().getMapper("field");
        if (fieldMapper.supportsParsingObject()) {
            Object sampleValueForDocument = getSampleObjectForDocument();
            assertThat(sampleValueForDocument, instanceOf(Map.class));
            SourceToParse source = source(builder -> {
                builder.field("field");
                builder.value(sampleValueForDocument);
            });
            ParsedDocument doc = mapper.parse(source);
            assertNotNull(doc);
        } else {
            expectThrows(Exception.class, () -> mapper.parse(source(b -> {
                b.startObject("field");
                b.endObject();
            })));
        }
    }

    public final void testSyntheticSourceMany() throws IOException {
        boolean ignoreMalformed = shouldUseIgnoreMalformed();
        int maxValues = randomBoolean() ? 1 : 5;
        SyntheticSourceSupport support = syntheticSourceSupport(ignoreMalformed);
        DocumentMapper mapper = createSytheticSourceMapperService(mapping(b -> {
            b.startObject("field");
            support.example(maxValues).mapping().accept(b);
            b.endObject();
        })).documentMapper();
        int count = between(2, 1000);
        String[] expected = new String[count];
        try (Directory directory = newDirectory()) {
            try (
                RandomIndexWriter iw = new RandomIndexWriter(
                    random(),
                    directory,
                    LuceneTestCase.newIndexWriterConfig(random(), new MockAnalyzer(random())).setMergePolicy(NoMergePolicy.INSTANCE)
                )
            ) {
                for (int i = 0; i < count; i++) {
                    if (rarely() && supportsEmptyInputArray()) {
                        expected[i] = support.preservesExactSource() ? "{\"field\":[]}" : "{}";
                        iw.addDocument(mapper.parse(source(b -> b.startArray("field").endArray())).rootDoc());
                        continue;
                    }
                    SyntheticSourceExample example = support.example(maxValues);
                    expected[i] = example.expected();
                    iw.addDocument(mapper.parse(source(example::buildInput)).rootDoc());
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                int i = 0;
                SourceLoader loader = mapper.mappers().newSourceLoader(null, SourceFieldMetrics.NOOP);
                StoredFieldLoader storedFieldLoader = loader.requiredStoredFields().isEmpty()
                    ? StoredFieldLoader.empty()
                    : StoredFieldLoader.create(false, loader.requiredStoredFields());
                for (LeafReaderContext leaf : reader.leaves()) {
                    int[] docIds = IntStream.range(0, leaf.reader().maxDoc()).toArray();
                    SourceLoader.Leaf sourceLoaderLeaf = loader.leaf(leaf.reader(), docIds);
                    LeafStoredFieldLoader storedLeaf = storedFieldLoader.getLoader(leaf, docIds);
                    for (int docId : docIds) {
                        storedLeaf.advanceTo(docId);
                        String source = sourceLoaderLeaf.source(storedLeaf, docId).internalSourceRef().utf8ToString();
                        assertThat("doc " + docId, source, equalTo(expected[i++]));
                    }
                }
            }
        }
    }

    public final void testNoSyntheticSourceForScript() throws IOException {
        // Fetch the ingest script support to eagerly assumeFalse if the mapper doesn't support ingest scripts
        ingestScriptSupport();
        DocumentMapper mapper = createSytheticSourceMapperService(mapping(b -> {
            b.startObject("field");
            minimalMapping(b);
            b.field("script", randomBoolean() ? "empty" : "non-empty");
            b.endObject();
        })).documentMapper();
        assertThat(syntheticSource(mapper, b -> {}), equalTo("{}"));
    }

    public final void testSyntheticSourceInObject() throws IOException {
        boolean ignoreMalformed = shouldUseIgnoreMalformed();
        SyntheticSourceExample syntheticSourceExample = syntheticSourceSupport(ignoreMalformed).example(5);
        DocumentMapper mapper = createSytheticSourceMapperService(mapping(b -> {
            b.startObject("obj").startObject("properties").startObject("field");
            syntheticSourceExample.mapping().accept(b);
            b.endObject().endObject().endObject();
        })).documentMapper();
        assertThat(syntheticSource(mapper, b -> {
            b.startObject("obj");
            syntheticSourceExample.buildInput(b);
            b.endObject();
        }), equalTo("{\"obj\":" + syntheticSourceExample.expected() + "}"));

        assertThat(syntheticSource(mapper, new SourceFilter(new String[] { "obj.field" }, null), b -> {
            b.startObject("obj");
            syntheticSourceExample.buildInput(b);
            b.endObject();
        }), equalTo("{\"obj\":" + syntheticSourceExample.expected() + "}"));

        assertThat(syntheticSource(mapper, new SourceFilter(null, new String[] { "obj.field" }), b -> {
            b.startObject("obj");
            syntheticSourceExample.buildInput(b);
            b.endObject();
        }), equalTo("{}"));
    }

    public final void testSyntheticEmptyList() throws IOException {
        assumeTrue("Field does not support [] as input", supportsEmptyInputArray());
        boolean ignoreMalformed = shouldUseIgnoreMalformed();
        SyntheticSourceSupport support = syntheticSourceSupport(ignoreMalformed);
        SyntheticSourceExample syntheticSourceExample = support.example(5);
        DocumentMapper mapper = createSytheticSourceMapperService(mapping(b -> {
            b.startObject("field");
            syntheticSourceExample.mapping().accept(b);
            b.endObject();
        })).documentMapper();

        var expected = support.preservesExactSource() ? "{\"field\":[]}" : "{}";
        assertThat(syntheticSource(mapper, b -> b.startArray("field").endArray()), equalTo(expected));
    }

    protected boolean shouldUseIgnoreMalformed() {
        // 5% of test runs use ignore_malformed
        return supportsIgnoreMalformed() && randomDouble() <= 0.05;
    }

    public final void testSyntheticEmptyListNoDocValuesLoader() throws IOException {
        assumeTrue("Field does not support [] as input", supportsEmptyInputArray());
        assertNoDocValueLoader(b -> b.startArray("field").endArray());
    }

    public final void testEmptyDocumentNoDocValueLoader() throws IOException {
        assumeFalse("Field will add values even if no fields are supplied", addsValueWhenNotSupplied());
        assertNoDocValueLoader(b -> {});
    }

    protected boolean addsValueWhenNotSupplied() {
        return false;
    }

    private void assertNoDocValueLoader(CheckedConsumer<XContentBuilder, IOException> doc) throws IOException {
        boolean ignoreMalformed = supportsIgnoreMalformed() ? rarely() : false;
        SyntheticSourceExample syntheticSourceExample = syntheticSourceSupport(ignoreMalformed).example(5);
        DocumentMapper mapper = createSytheticSourceMapperService(mapping(b -> {
            b.startObject("field");
            syntheticSourceExample.mapping().accept(b);
            b.endObject();
        })).documentMapper();
        try (Directory directory = newDirectory()) {
            RandomIndexWriter iw = new RandomIndexWriter(random(), directory);
            iw.addDocument(mapper.parse(source(doc)).rootDoc());
            iw.close();
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                LeafReader leafReader = getOnlyLeafReader(reader);
                SourceLoader.SyntheticFieldLoader fieldLoader = ((FieldMapper) mapper.mapping().getRoot().getMapper("field"))
                    .syntheticFieldLoader();
                /*
                 * null means "there are no values for this field, don't call me".
                 * Empty fields are common enough that we need to make sure this
                 * optimization kicks in.
                 */
                assertThat(fieldLoader.docValuesLoader(leafReader, new int[] { 0 }), nullValue());
            }
        }
    }

    public final void testSyntheticSourceInvalid() throws IOException {
        boolean ignoreMalformed = shouldUseIgnoreMalformed();
        List<SyntheticSourceInvalidExample> examples = new ArrayList<>(syntheticSourceSupport(ignoreMalformed).invalidExample());
        for (SyntheticSourceInvalidExample example : examples) {
            Exception e = expectThrows(
                IllegalArgumentException.class,
                example.toString(),
                () -> createSytheticSourceMapperService(mapping(b -> {
                    b.startObject("field");
                    example.mapping.accept(b);
                    b.endObject();
                }))
            );
            assertThat(e.getMessage(), example.error);
        }
    }

    public final void testSyntheticSourceInNestedObject() throws IOException {
        boolean ignoreMalformed = shouldUseIgnoreMalformed();
        SyntheticSourceExample syntheticSourceExample = syntheticSourceSupport(ignoreMalformed).example(5);
        DocumentMapper mapper = createSytheticSourceMapperService(mapping(b -> {
            b.startObject("obj").field("type", "nested").startObject("properties").startObject("field");
            syntheticSourceExample.mapping().accept(b);
            b.endObject().endObject().endObject();
        })).documentMapper();
        assertThat(syntheticSource(mapper, b -> {
            b.startObject("obj");
            syntheticSourceExample.buildInput(b);
            b.endObject();
        }), equalTo("{\"obj\":" + syntheticSourceExample.expected() + "}"));

        assertThat(syntheticSource(mapper, new SourceFilter(new String[] { "obj.field" }, null), b -> {
            b.startObject("obj");
            syntheticSourceExample.buildInput(b);
            b.endObject();
        }), equalTo("{\"obj\":" + syntheticSourceExample.expected() + "}"));

        assertThat(syntheticSource(mapper, new SourceFilter(null, new String[] { "obj.field" }), b -> {
            b.startObject("obj");
            syntheticSourceExample.buildInput(b);
            b.endObject();
        }), equalTo("{\"obj\":{}}"));

        assertThat(syntheticSource(mapper, new SourceFilter(null, new String[] { "obj" }), b -> {
            b.startObject("obj");
            syntheticSourceExample.buildInput(b);
            b.endObject();
        }), equalTo("{}"));
    }

    protected SyntheticSourceSupport syntheticSourceSupportForKeepTests(boolean ignoreMalformed, Mapper.SourceKeepMode keepMode) {
        return syntheticSourceSupport(ignoreMalformed);
    }

    public void testSyntheticSourceKeepNone() throws IOException {
        SyntheticSourceExample example = syntheticSourceSupportForKeepTests(shouldUseIgnoreMalformed(), Mapper.SourceKeepMode.NONE).example(
            1
        );
        DocumentMapper mapper = createSytheticSourceMapperService(mapping(b -> {
            b.startObject("field");
            b.field("synthetic_source_keep", "none");
            example.mapping().accept(b);
            b.endObject();
        })).documentMapper();
        assertThat(syntheticSource(mapper, example::buildInput), equalTo(example.expected()));
    }

    public void testSyntheticSourceKeepAll() throws IOException {
        SyntheticSourceExample example = syntheticSourceSupportForKeepTests(shouldUseIgnoreMalformed(), Mapper.SourceKeepMode.ALL).example(
            1
        );
        DocumentMapper mapperAll = createSytheticSourceMapperService(mapping(b -> {
            b.startObject("field");
            b.field("synthetic_source_keep", "all");
            example.mapping().accept(b);
            b.endObject();
        })).documentMapper();

        var builder = XContentFactory.jsonBuilder();
        builder.startObject();
        example.buildInput(builder);
        builder.endObject();
        String expected = Strings.toString(builder);
        assertThat(syntheticSource(mapperAll, example::buildInput), equalTo(expected));
    }

    public void testSyntheticSourceKeepArrays() throws IOException {
        SyntheticSourceExample example = syntheticSourceSupportForKeepTests(shouldUseIgnoreMalformed(), Mapper.SourceKeepMode.ARRAYS)
            .example(1);
        DocumentMapper mapperAll = createSytheticSourceMapperService(mapping(b -> {
            b.startObject("field");
            b.field("synthetic_source_keep", randomSyntheticSourceKeep());
            example.mapping().accept(b);
            b.endObject();
        })).documentMapper();

        int elementCount = randomIntBetween(2, 5);
        CheckedConsumer<XContentBuilder, IOException> buildInput = (XContentBuilder builder) -> {
            example.buildInputArray(builder, elementCount);
        };

        var builder = XContentFactory.jsonBuilder();
        builder.startObject();
        buildInput.accept(builder);
        builder.endObject();
        String expected = Strings.toString(builder);
        String actual = syntheticSource(mapperAll, buildInput);
        assertThat(actual, equalTo(expected));
    }

    protected String randomSyntheticSourceKeep() {
        return randomFrom("all", "arrays");
    }

    @Override
    protected final <T> T compileScript(Script script, ScriptContext<T> context) {
        return ingestScriptSupport().compileScript(script, context);
    }

    protected abstract IngestScriptSupport ingestScriptSupport();

    protected abstract class IngestScriptSupport {
        private <T> T compileScript(Script script, ScriptContext<T> context) {
            switch (script.getIdOrCode()) {
                case "empty":
                    return context.factoryClazz.cast(emptyFieldScript());
                case "non-empty":
                    return context.factoryClazz.cast(nonEmptyFieldScript());
                default:
                    return compileOtherScript(script, context);
            }
        }

        protected <T> T compileOtherScript(Script script, ScriptContext<T> context) {
            throw new UnsupportedOperationException("Unknown script " + script.getIdOrCode());
        }

        /**
         * Create a script that can be run to produce no values for this
         * field or return {@link Optional#empty()} to signal that this
         * field doesn't support fields scripts.
         */
        abstract ScriptFactory emptyFieldScript();

        /**
         * Create a script that can be run to produce some value value for this
         * field or return {@link Optional#empty()} to signal that this
         * field doesn't support fields scripts.
         */
        abstract ScriptFactory nonEmptyFieldScript();
    }
}
