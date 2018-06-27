package org.elasticsearch.indexing;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.test.ESIntegTestCase;

public class IndexCreationIT extends ESIntegTestCase {
    public void testRepeatedCreationAndDeletion() {

        final int repeats = 100;

        ActionFuture<DeleteIndexResponse> deleteIndexResponseActionFuture = null;

        for (int i = 0; i < repeats; i++) {
            final ActionFuture<CreateIndexResponse> createIndexResponseActionFuture = prepareCreate("test").execute();

            ensureGreen();
            try {
                createIndexResponseActionFuture.get();
            } catch (Exception e) {
                // don't care
            }

            if (deleteIndexResponseActionFuture != null) {
                try {
                    deleteIndexResponseActionFuture.get();
                } catch (Exception e) {
                    // don't care
                }
            }

            deleteIndexResponseActionFuture = client().admin().indices().prepareDelete("test").execute();
        }

        try {
            deleteIndexResponseActionFuture.get();
        } catch (Exception e) {
            // don't care
        }
    }
}
