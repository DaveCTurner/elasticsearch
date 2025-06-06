/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MetadataCreateDataStreamService;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.mapper.DataStreamTimestampFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.MappingParserContext;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.ProvidedIdFieldMapper;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.mapper.RoutingFields;
import org.elasticsearch.index.mapper.RoutingPathFields;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesRoutingHashFieldMapper;
import org.elasticsearch.index.mapper.TsidExtractingIdFieldMapper;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;

/**
 * "Mode" that controls which behaviors and settings an index supports.
 * <p>
 * For the most part this class concentrates on validating settings and
 * mappings. Most different behavior is controlled by forcing settings
 * to be set or not set and by enabling extra fields in the mapping.
 */
public enum IndexMode {
    STANDARD("standard") {
        @Override
        void validateWithOtherSettings(Map<Setting<?>, Object> settings) {
            validateRoutingPathSettings(settings);
            validateTimeSeriesSettings(settings);
        }

        @Override
        public void validateMapping(MappingLookup lookup) {};

        @Override
        public void validateAlias(@Nullable String indexRouting, @Nullable String searchRouting) {}

        @Override
        public void validateTimestampFieldMapping(boolean isDataStream, MappingLookup mappingLookup) throws IOException {
            if (isDataStream) {
                MetadataCreateDataStreamService.validateTimestampFieldMapping(mappingLookup);
            }
        }

        @Override
        public CompressedXContent getDefaultMapping(final IndexSettings indexSettings) {
            return null;
        }

        @Override
        public TimestampBounds getTimestampBound(IndexMetadata indexMetadata) {
            return null;
        }

        @Override
        public MetadataFieldMapper timeSeriesIdFieldMapper(MappingParserContext c) {
            // non time-series indices must not have a TimeSeriesIdFieldMapper
            return null;
        }

        @Override
        public MetadataFieldMapper timeSeriesRoutingHashFieldMapper() {
            // non time-series indices must not have a TimeSeriesRoutingIdFieldMapper
            return null;
        }

        @Override
        public IdFieldMapper idFieldMapperWithoutFieldData() {
            return ProvidedIdFieldMapper.NO_FIELD_DATA;
        }

        @Override
        public IdFieldMapper buildIdFieldMapper(BooleanSupplier fieldDataEnabled) {
            return new ProvidedIdFieldMapper(fieldDataEnabled);
        }

        @Override
        public RoutingFields buildRoutingFields(IndexSettings settings) {
            return RoutingFields.Noop.INSTANCE;
        }

        @Override
        public boolean shouldValidateTimestamp() {
            return false;
        }

        @Override
        public void validateSourceFieldMapper(SourceFieldMapper sourceFieldMapper) {}

        @Override
        public SourceFieldMapper.Mode defaultSourceMode() {
            return SourceFieldMapper.Mode.STORED;
        }

        @Override
        public boolean useDefaultPostingsFormat() {
            return true;
        }
    },
    TIME_SERIES("time_series") {
        @Override
        void validateWithOtherSettings(Map<Setting<?>, Object> settings) {
            if (settings.get(IndexMetadata.INDEX_ROUTING_PARTITION_SIZE_SETTING) != Integer.valueOf(1)) {
                throw new IllegalArgumentException(error(IndexMetadata.INDEX_ROUTING_PARTITION_SIZE_SETTING));
            }
            for (Setting<?> unsupported : TIME_SERIES_UNSUPPORTED) {
                if (false == Objects.equals(unsupported.getDefault(Settings.EMPTY), settings.get(unsupported))) {
                    throw new IllegalArgumentException(error(unsupported));
                }
            }
            checkSetting(settings, IndexMetadata.INDEX_ROUTING_PATH);
        }

        private static void checkSetting(Map<Setting<?>, Object> settings, Setting<?> setting) {
            if (Objects.equals(setting.getDefault(Settings.EMPTY), settings.get(setting))) {
                throw new IllegalArgumentException(tsdbMode() + " requires a non-empty [" + setting.getKey() + "]");
            }
        }

        private static String error(Setting<?> unsupported) {
            return tsdbMode() + " is incompatible with [" + unsupported.getKey() + "]";
        }

        @Override
        public void validateMapping(MappingLookup lookup) {
            if (((RoutingFieldMapper) lookup.getMapper(RoutingFieldMapper.NAME)).required()) {
                throw new IllegalArgumentException(routingRequiredBad());
            }
        }

        @Override
        public void validateAlias(@Nullable String indexRouting, @Nullable String searchRouting) {
            if (indexRouting != null || searchRouting != null) {
                throw new IllegalArgumentException(routingRequiredBad());
            }
        }

        @Override
        public void validateTimestampFieldMapping(boolean isDataStream, MappingLookup mappingLookup) throws IOException {
            MetadataCreateDataStreamService.validateTimestampFieldMapping(mappingLookup);
        }

        @Override
        public CompressedXContent getDefaultMapping(final IndexSettings indexSettings) {
            return DEFAULT_MAPPING_TIMESTAMP;
        }

        @Override
        public TimestampBounds getTimestampBound(IndexMetadata indexMetadata) {
            return new TimestampBounds(indexMetadata.getTimeSeriesStart(), indexMetadata.getTimeSeriesEnd());
        }

        private static String routingRequiredBad() {
            return "routing is forbidden on CRUD operations that target indices in " + tsdbMode();
        }

        @Override
        public MetadataFieldMapper timeSeriesIdFieldMapper(MappingParserContext c) {
            return TimeSeriesIdFieldMapper.getInstance(c);
        }

        @Override
        public MetadataFieldMapper timeSeriesRoutingHashFieldMapper() {
            return TimeSeriesRoutingHashFieldMapper.INSTANCE;
        }

        public IdFieldMapper idFieldMapperWithoutFieldData() {
            return TsidExtractingIdFieldMapper.INSTANCE;
        }

        @Override
        public IdFieldMapper buildIdFieldMapper(BooleanSupplier fieldDataEnabled) {
            // We don't support field data on TSDB's _id
            return TsidExtractingIdFieldMapper.INSTANCE;
        }

        @Override
        public RoutingFields buildRoutingFields(IndexSettings settings) {
            IndexRouting.ExtractFromSource routing = (IndexRouting.ExtractFromSource) settings.getIndexRouting();
            return new RoutingPathFields(routing.builder());
        }

        @Override
        public boolean shouldValidateTimestamp() {
            return true;
        }

        @Override
        public void validateSourceFieldMapper(SourceFieldMapper sourceFieldMapper) {
            if (sourceFieldMapper.enabled() == false) {
                throw new IllegalArgumentException("_source can not be disabled in index using [" + IndexMode.TIME_SERIES + "] index mode");
            }
        }

        @Override
        public SourceFieldMapper.Mode defaultSourceMode() {
            return SourceFieldMapper.Mode.SYNTHETIC;
        }
    },
    LOGSDB("logsdb") {
        @Override
        void validateWithOtherSettings(Map<Setting<?>, Object> settings) {
            validateTimeSeriesSettings(settings);
            var setting = settings.get(IndexSettings.LOGSDB_ROUTE_ON_SORT_FIELDS);
            if (setting.equals(Boolean.FALSE)) {
                validateRoutingPathSettings(settings);
            }
        }

        @Override
        public void validateMapping(MappingLookup lookup) {}

        @Override
        public void validateAlias(String indexRouting, String searchRouting) {

        }

        @Override
        public void validateTimestampFieldMapping(boolean isDataStream, MappingLookup mappingLookup) throws IOException {
            if (isDataStream) {
                MetadataCreateDataStreamService.validateTimestampFieldMapping(mappingLookup);
            }
        }

        @Override
        public CompressedXContent getDefaultMapping(final IndexSettings indexSettings) {
            return indexSettings != null && indexSettings.logsdbAddHostNameField()
                ? DEFAULT_MAPPING_TIMESTAMP_HOSTNAME
                : DEFAULT_MAPPING_TIMESTAMP;
        }

        @Override
        public IdFieldMapper buildIdFieldMapper(BooleanSupplier fieldDataEnabled) {
            return new ProvidedIdFieldMapper(fieldDataEnabled);
        }

        @Override
        public IdFieldMapper idFieldMapperWithoutFieldData() {
            return ProvidedIdFieldMapper.NO_FIELD_DATA;
        }

        @Override
        public TimestampBounds getTimestampBound(IndexMetadata indexMetadata) {
            return null;
        }

        @Override
        public MetadataFieldMapper timeSeriesIdFieldMapper(MappingParserContext c) {
            // non time-series indices must not have a TimeSeriesIdFieldMapper
            return null;
        }

        @Override
        public MetadataFieldMapper timeSeriesRoutingHashFieldMapper() {
            // non time-series indices must not have a TimeSeriesRoutingIdFieldMapper
            return null;
        }

        @Override
        public RoutingFields buildRoutingFields(IndexSettings settings) {
            return RoutingFields.Noop.INSTANCE;
        }

        @Override
        public boolean shouldValidateTimestamp() {
            return false;
        }

        @Override
        public void validateSourceFieldMapper(SourceFieldMapper sourceFieldMapper) {
            if (sourceFieldMapper.enabled() == false) {
                throw new IllegalArgumentException("_source can not be disabled in index using [" + IndexMode.LOGSDB + "] index mode");
            }
        }

        @Override
        public SourceFieldMapper.Mode defaultSourceMode() {
            return SourceFieldMapper.Mode.SYNTHETIC;
        }

        @Override
        public String getDefaultCodec() {
            return CodecService.BEST_COMPRESSION_CODEC;
        }
    },
    LOOKUP("lookup") {
        @Override
        void validateWithOtherSettings(Map<Setting<?>, Object> settings) {
            final Integer providedNumberOfShards = (Integer) settings.get(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING);
            if (providedNumberOfShards != null && providedNumberOfShards != 1) {
                throw new IllegalArgumentException(
                    "index with [lookup] mode must have [index.number_of_shards] set to 1 or unset; provided " + providedNumberOfShards
                );
            }
        }

        @Override
        public void validateMapping(MappingLookup lookup) {};

        @Override
        public void validateAlias(@Nullable String indexRouting, @Nullable String searchRouting) {}

        @Override
        public void validateTimestampFieldMapping(boolean isDataStream, MappingLookup mappingLookup) {

        }

        @Override
        public CompressedXContent getDefaultMapping(final IndexSettings indexSettings) {
            return null;
        }

        @Override
        public TimestampBounds getTimestampBound(IndexMetadata indexMetadata) {
            return null;
        }

        @Override
        public MetadataFieldMapper timeSeriesIdFieldMapper(MappingParserContext c) {
            // non time-series indices must not have a TimeSeriesIdFieldMapper
            return null;
        }

        @Override
        public MetadataFieldMapper timeSeriesRoutingHashFieldMapper() {
            // non time-series indices must not have a TimeSeriesRoutingIdFieldMapper
            return null;
        }

        @Override
        public IdFieldMapper idFieldMapperWithoutFieldData() {
            return ProvidedIdFieldMapper.NO_FIELD_DATA;
        }

        @Override
        public IdFieldMapper buildIdFieldMapper(BooleanSupplier fieldDataEnabled) {
            return new ProvidedIdFieldMapper(fieldDataEnabled);
        }

        @Override
        public RoutingFields buildRoutingFields(IndexSettings settings) {
            return RoutingFields.Noop.INSTANCE;
        }

        @Override
        public boolean shouldValidateTimestamp() {
            return false;
        }

        @Override
        public void validateSourceFieldMapper(SourceFieldMapper sourceFieldMapper) {}

        @Override
        public SourceFieldMapper.Mode defaultSourceMode() {
            return SourceFieldMapper.Mode.STORED;
        }
    };

    static final String HOST_NAME = "host.name";

    private static void validateRoutingPathSettings(Map<Setting<?>, Object> settings) {
        settingRequiresTimeSeries(settings, IndexMetadata.INDEX_ROUTING_PATH);
    }

    private static void validateTimeSeriesSettings(Map<Setting<?>, Object> settings) {
        settingRequiresTimeSeries(settings, IndexSettings.TIME_SERIES_START_TIME);
        settingRequiresTimeSeries(settings, IndexSettings.TIME_SERIES_END_TIME);
    }

    private static void settingRequiresTimeSeries(Map<Setting<?>, Object> settings, Setting<?> setting) {
        if (false == Objects.equals(setting.getDefault(Settings.EMPTY), settings.get(setting))) {
            throw new IllegalArgumentException("[" + setting.getKey() + "] requires " + tsdbMode());
        }
    }

    protected static String tsdbMode() {
        return "[" + IndexSettings.MODE.getKey() + "=time_series]";
    }

    private static CompressedXContent createDefaultMapping(boolean includeHostName) throws IOException {
        return new CompressedXContent((builder, params) -> {
            builder.startObject(MapperService.SINGLE_MAPPING_NAME)
                .startObject(DataStreamTimestampFieldMapper.NAME)
                .field("enabled", true)
                .endObject()
                .startObject("properties")
                .startObject(DataStreamTimestampFieldMapper.DEFAULT_PATH)
                .field("type", DateFieldMapper.CONTENT_TYPE)
                .endObject();

            if (includeHostName) {
                builder.startObject(HOST_NAME).field("type", KeywordFieldMapper.CONTENT_TYPE).field("ignore_above", 1024).endObject();
            }

            return builder.endObject().endObject();
        });
    }

    private static final CompressedXContent DEFAULT_MAPPING_TIMESTAMP;

    private static final CompressedXContent DEFAULT_MAPPING_TIMESTAMP_HOSTNAME;

    static {
        try {
            DEFAULT_MAPPING_TIMESTAMP = createDefaultMapping(false);
            DEFAULT_MAPPING_TIMESTAMP_HOSTNAME = createDefaultMapping(true);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private static final List<Setting<?>> TIME_SERIES_UNSUPPORTED = List.of(
        IndexSortConfig.INDEX_SORT_FIELD_SETTING,
        IndexSortConfig.INDEX_SORT_ORDER_SETTING,
        IndexSortConfig.INDEX_SORT_MODE_SETTING,
        IndexSortConfig.INDEX_SORT_MISSING_SETTING
    );

    static final List<Setting<?>> VALIDATE_WITH_SETTINGS = List.copyOf(
        Stream.concat(
            Stream.of(
                IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING,
                IndexMetadata.INDEX_ROUTING_PARTITION_SIZE_SETTING,
                IndexMetadata.INDEX_ROUTING_PATH,
                IndexSettings.LOGSDB_ROUTE_ON_SORT_FIELDS,
                IndexSettings.TIME_SERIES_START_TIME,
                IndexSettings.TIME_SERIES_END_TIME
            ),
            TIME_SERIES_UNSUPPORTED.stream()
        ).collect(toSet())
    );

    private final String name;

    IndexMode(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    abstract void validateWithOtherSettings(Map<Setting<?>, Object> settings);

    /**
     * Validate the mapping for this index.
     */
    public abstract void validateMapping(MappingLookup lookup);

    /**
     * Validate aliases targeting this index.
     */
    public abstract void validateAlias(@Nullable String indexRouting, @Nullable String searchRouting);

    /**
     * validate timestamp mapping for this index.
     */
    public abstract void validateTimestampFieldMapping(boolean isDataStream, MappingLookup mappingLookup) throws IOException;

    /**
     * Get default mapping for this index or {@code null} if there is none.
     */
    @Nullable
    public abstract CompressedXContent getDefaultMapping(IndexSettings indexSettings);

    /**
     * Build the {@link FieldMapper} for {@code _id}.
     */
    public abstract IdFieldMapper buildIdFieldMapper(BooleanSupplier fieldDataEnabled);

    /**
     * Get the singleton {@link FieldMapper} for {@code _id}. It can never support
     * field data.
     */
    public abstract IdFieldMapper idFieldMapperWithoutFieldData();

    /**
     * @return the time range based on the provided index metadata and index mode implementation.
     * Otherwise <code>null</code> is returned.
     */
    @Nullable
    public abstract TimestampBounds getTimestampBound(IndexMetadata indexMetadata);

    /**
     * Return an instance of the {@link TimeSeriesIdFieldMapper} that generates
     * the _tsid field. The field mapper will be added to the list of the metadata
     * field mappers for the index.
     */
    public abstract MetadataFieldMapper timeSeriesIdFieldMapper(MappingParserContext c);

    /**
     * Return an instance of the {@link TimeSeriesRoutingHashFieldMapper} that generates
     * the _ts_routing_hash field. The field mapper will be added to the list of the metadata
     * field mappers for the index.
     */
    public abstract MetadataFieldMapper timeSeriesRoutingHashFieldMapper();

    /**
     * How {@code time_series_dimension} fields are handled by indices in this mode.
     */
    public abstract RoutingFields buildRoutingFields(IndexSettings settings);

    /**
     * @return Whether timestamps should be validated for being withing the time range of an index.
     */
    public abstract boolean shouldValidateTimestamp();

    /**
     * Validates the source field mapper
     */
    public abstract void validateSourceFieldMapper(SourceFieldMapper sourceFieldMapper);

    /**
     * @return default source mode for this mode
     */
    public abstract SourceFieldMapper.Mode defaultSourceMode();

    public String getDefaultCodec() {
        return CodecService.DEFAULT_CODEC;
    }

    /**
     * Whether the default posting format (for inverted indices) from Lucene should be used.
     */
    public boolean useDefaultPostingsFormat() {
        return false;
    }

    /**
     * Parse a string into an {@link IndexMode}.
     */
    public static IndexMode fromString(String value) {
        return switch (value) {
            case "standard" -> IndexMode.STANDARD;
            case "time_series" -> IndexMode.TIME_SERIES;
            case "logsdb" -> IndexMode.LOGSDB;
            case "lookup" -> IndexMode.LOOKUP;
            default -> throw new IllegalArgumentException(
                "["
                    + value
                    + "] is an invalid index mode, valid modes are: ["
                    + Arrays.stream(IndexMode.values()).map(IndexMode::toString).collect(Collectors.joining(","))
                    + "]"
            );
        };
    }

    public static IndexMode readFrom(StreamInput in) throws IOException {
        int mode = in.readByte();
        return switch (mode) {
            case 0 -> STANDARD;
            case 1 -> TIME_SERIES;
            case 2 -> LOGSDB;
            case 3 -> LOOKUP;
            default -> throw new IllegalStateException("unexpected index mode [" + mode + "]");
        };
    }

    public static void writeTo(IndexMode indexMode, StreamOutput out) throws IOException {
        final int code = switch (indexMode) {
            case STANDARD -> 0;
            case TIME_SERIES -> 1;
            case LOGSDB -> 2;
            case LOOKUP -> out.getTransportVersion().onOrAfter(TransportVersions.V_8_17_0) ? 3 : 0;
        };
        out.writeByte((byte) code);
    }

    @Override
    public String toString() {
        return getName();
    }

    /**
     * A built-in index setting provider that supplies additional index settings based on the index mode.
     * Currently, only the lookup index mode provides non-empty additional settings.
     */
    public static final class IndexModeSettingsProvider implements IndexSettingProvider {
        @Override
        public Settings getAdditionalIndexSettings(
            String indexName,
            String dataStreamName,
            IndexMode templateIndexMode,
            ProjectMetadata projectMetadata,
            Instant resolvedAt,
            Settings indexTemplateAndCreateRequestSettings,
            List<CompressedXContent> combinedTemplateMappings
        ) {
            IndexMode indexMode = templateIndexMode;
            if (indexMode == null) {
                String modeName = indexTemplateAndCreateRequestSettings.get(IndexSettings.MODE.getKey());
                if (modeName != null) {
                    indexMode = IndexMode.valueOf(modeName.toUpperCase(Locale.ROOT));
                }
            }
            if (indexMode == LOOKUP) {
                return Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).build();
            } else {
                return Settings.EMPTY;
            }
        }
    }
}
