package org.elasticsearch.indexing;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.List;

public class IndexCreationIT extends ESIntegTestCase {
    public void testRepeatedCreationAndDeletion() {

        final int repeats = 10000;

        final List<ActionFuture<CreateIndexResponse>> createIndexResponses = new ArrayList<>(repeats);
        final List<ActionFuture<DeleteIndexResponse>> deleteIndexResponses = new ArrayList<>(repeats);

        for (int i = 0; i < repeats; i++) {
             createIndexResponses.add(prepareCreate("test").execute());
             deleteIndexResponses.add(client().admin().indices().prepareDelete("test").execute());
        }

        for (ActionFuture<CreateIndexResponse> createIndexResponse : createIndexResponses) {
            try {
                createIndexResponse.get();
            } catch (Exception e) {
                // don't care
            }
        }

        for (ActionFuture<DeleteIndexResponse> deleteIndexResponse : deleteIndexResponses) {
            try {
                deleteIndexResponse.get();
            } catch (Exception e) {
                // don't care
            }
        }

        ensureGreen();
    }
}
