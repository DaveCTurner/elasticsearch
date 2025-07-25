/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * this file has been contributed to by a Generative AI
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.inference.TaskType;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.InferenceBaseRestTest.getAllModels;
import static org.elasticsearch.xpack.inference.InferenceBaseRestTest.getModels;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class InferenceGetModelsWithElasticInferenceServiceIT extends BaseMockEISAuthServerTest {

    @BeforeClass
    public static void init() {
        // Ensure the mock EIS server has an authorized response ready
        mockEISServer.enqueueAuthorizeAllModelsResponse();
    }

    public void testGetDefaultEndpoints() throws IOException {
        var allModels = getAllModels();
        var chatCompletionModels = getModels("_all", TaskType.CHAT_COMPLETION);

        assertThat(allModels, hasSize(7));
        assertThat(chatCompletionModels, hasSize(1));

        for (var model : chatCompletionModels) {
            assertEquals("chat_completion", model.get("task_type"));
        }

        assertInferenceIdTaskType(allModels, ".rainbow-sprinkles-elastic", TaskType.CHAT_COMPLETION);
        assertInferenceIdTaskType(allModels, ".elser-2-elastic", TaskType.SPARSE_EMBEDDING);
        assertInferenceIdTaskType(allModels, ".multilingual-embed-v1-elastic", TaskType.TEXT_EMBEDDING);
        assertInferenceIdTaskType(allModels, ".rerank-v1-elastic", TaskType.RERANK);
    }

    private static void assertInferenceIdTaskType(List<Map<String, Object>> models, String inferenceId, TaskType taskType) {
        var model = models.stream().filter(m -> m.get("inference_id").equals(inferenceId)).findFirst();
        assertTrue("could not find inference id: " + inferenceId, model.isPresent());
        assertThat(model.get().get("task_type"), is(taskType.toString()));
    }
}
