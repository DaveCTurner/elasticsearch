package org.elasticsearch.indexing;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.List;

public class IndexCreationIT extends ESIntegTestCase {
    public void testRepeatedCreationAndDeletion() {

        final int repeats = 100;

        final List<ActionFuture<CreateIndexResponse>> createIndexResponses = new ArrayList<>(repeats);
        final List<ActionFuture<DeleteIndexResponse>> deleteIndexResponses = new ArrayList<>(repeats);

        for (int i = 0; i < repeats; i++) {
             createIndexResponses.add(prepareCreate("test").execute());
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                // don't care
            }
            deleteIndexResponses.add(client().admin().indices().prepareDelete("test").execute());
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                // don't care
            }
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
