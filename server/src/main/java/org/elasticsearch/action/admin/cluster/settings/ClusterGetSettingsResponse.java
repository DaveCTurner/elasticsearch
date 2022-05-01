/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.settings;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.Objects;

public class ClusterGetSettingsResponse extends ActionResponse {

    private final Settings persistentSettings;
    private final Settings transientSettings;
    private final Settings combinedSettings;

    public ClusterGetSettingsResponse(Settings persistentSettings, Settings transientSettings, Settings combinedSettings) {
        this.persistentSettings = Objects.requireNonNull(persistentSettings);
        this.transientSettings = Objects.requireNonNull(transientSettings);
        this.combinedSettings = Objects.requireNonNull(combinedSettings);
    }

    public ClusterGetSettingsResponse(StreamInput in) throws IOException {
        assert in.getVersion().onOrAfter(Version.V_8_3_0) : in.getVersion();
        persistentSettings = Settings.readSettingsFromStream(in);
        transientSettings = Settings.readSettingsFromStream(in);
        combinedSettings = Settings.readSettingsFromStream(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        assert out.getVersion().onOrAfter(Version.V_8_3_0) : out.getVersion();
        Settings.writeSettingsToStream(persistentSettings, out);
        Settings.writeSettingsToStream(transientSettings, out);
        Settings.writeSettingsToStream(combinedSettings, out);
    }

    public Settings persistentSettings() {
        return persistentSettings;
    }

    public Settings transientSettings() {
        return transientSettings;
    }

    public Settings combinedSettings() {
        return combinedSettings;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterGetSettingsResponse that = (ClusterGetSettingsResponse) o;
        return persistentSettings.equals(that.persistentSettings)
            && transientSettings.equals(that.transientSettings)
            && combinedSettings.equals(that.combinedSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(persistentSettings, transientSettings, combinedSettings);
    }
}
