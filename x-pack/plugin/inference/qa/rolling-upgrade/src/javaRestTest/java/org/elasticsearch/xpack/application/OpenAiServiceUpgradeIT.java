/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.application;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class OpenAiServiceUpgradeIT extends InferenceUpgradeTestCase {

    // TODO: replace with proper test features
    private static final String OPEN_AI_EMBEDDINGS_TEST_FEATURE = "gte_v8.12.0";
    private static final String OPEN_AI_EMBEDDINGS_MODEL_SETTING_MOVED_TEST_FEATURE = "gte_v8.13.0";
    private static final String OPEN_AI_COMPLETIONS_TEST_FEATURE = "gte_v8.14.0";

    private static MockWebServer openAiEmbeddingsServer;
    private static MockWebServer openAiChatCompletionsServer;

    public OpenAiServiceUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    @BeforeClass
    public static void startWebServer() throws IOException {
        openAiEmbeddingsServer = new MockWebServer();
        openAiEmbeddingsServer.start();

        openAiChatCompletionsServer = new MockWebServer();
        openAiChatCompletionsServer.start();
    }

    @AfterClass
    public static void shutdown() {
        openAiEmbeddingsServer.close();
        openAiChatCompletionsServer.close();
    }

    @SuppressWarnings("unchecked")
    public void testOpenAiEmbeddings() throws IOException {
        var openAiEmbeddingsSupported = oldClusterHasFeature(OPEN_AI_EMBEDDINGS_TEST_FEATURE);
        String oldClusterEndpointIdentifier = oldClusterHasFeature(MODELS_RENAMED_TO_ENDPOINTS_FEATURE) ? "endpoints" : "models";
        assumeTrue("OpenAI embedding service supported", openAiEmbeddingsSupported);

        final String oldClusterId = "old-cluster-embeddings";
        final String upgradedClusterId = "upgraded-cluster-embeddings";

        var testTaskType = TaskType.TEXT_EMBEDDING;

        if (isOldCluster()) {
            String inferenceConfig = oldClusterVersionCompatibleEmbeddingConfig();
            // queue a response as PUT will call the service
            openAiEmbeddingsServer.enqueue(new MockResponse().setResponseCode(200).setBody(embeddingResponse()));
            put(oldClusterId, inferenceConfig, testTaskType);

            var configs = (List<Map<String, Object>>) get(testTaskType, oldClusterId).get(oldClusterEndpointIdentifier);
            assertThat(configs, hasSize(1));

            assertEmbeddingInference(oldClusterId);
        } else if (isMixedCluster()) {
            var configs = getConfigsWithBreakingChangeHandling(testTaskType, oldClusterId);

            assertEquals("openai", configs.get(0).get("service"));
            var serviceSettings = (Map<String, Object>) configs.get(0).get("service_settings");
            var taskSettings = (Map<String, Object>) configs.get(0).get("task_settings");
            var modelIdFound = serviceSettings.containsKey("model_id")
                || (taskSettings.containsKey("model")
                    && (oldClusterHasFeature(OPEN_AI_EMBEDDINGS_MODEL_SETTING_MOVED_TEST_FEATURE) == false));
            assertTrue("model_id not found in config: " + configs, modelIdFound);

            assertEmbeddingInference(oldClusterId);
        } else if (isUpgradedCluster()) {
            // check old cluster model
            var configs = (List<Map<String, Object>>) get(testTaskType, oldClusterId).get("endpoints");
            var serviceSettings = (Map<String, Object>) configs.get(0).get("service_settings");
            // model id is moved to service settings
            assertThat(serviceSettings, hasEntry("model_id", "text-embedding-ada-002"));
            var taskSettings = (Map<String, Object>) configs.get(0).get("task_settings");
            assertThat(taskSettings, anyOf(nullValue(), anEmptyMap()));

            // Inference on old cluster model
            assertEmbeddingInference(oldClusterId);

            String inferenceConfig = embeddingConfigWithModelInServiceSettings(getUrl(openAiEmbeddingsServer));
            openAiEmbeddingsServer.enqueue(new MockResponse().setResponseCode(200).setBody(embeddingResponse()));
            put(upgradedClusterId, inferenceConfig, testTaskType);

            configs = (List<Map<String, Object>>) get(testTaskType, upgradedClusterId).get("endpoints");
            assertThat(configs, hasSize(1));

            assertEmbeddingInference(upgradedClusterId);

            delete(oldClusterId);
            delete(upgradedClusterId);
        }
    }

    void assertEmbeddingInference(String inferenceId) throws IOException {
        openAiEmbeddingsServer.enqueue(new MockResponse().setResponseCode(200).setBody(embeddingResponse()));
        var inferenceMap = inference(inferenceId, TaskType.TEXT_EMBEDDING, "some text");
        assertThat(inferenceMap.entrySet(), not(empty()));
    }

    @SuppressWarnings("unchecked")
    public void testOpenAiCompletions() throws IOException {
        var openAiEmbeddingsSupported = oldClusterHasFeature(OPEN_AI_COMPLETIONS_TEST_FEATURE);
        String old_cluster_endpoint_identifier = oldClusterHasFeature(MODELS_RENAMED_TO_ENDPOINTS_FEATURE) ? "endpoints" : "models";
        assumeTrue("OpenAI completions service supported" + OPEN_AI_COMPLETIONS_TEST_FEATURE, openAiEmbeddingsSupported);

        final String oldClusterId = "old-cluster-completions";
        final String upgradedClusterId = "upgraded-cluster-completions";

        var testTaskType = TaskType.COMPLETION;

        if (isOldCluster()) {
            openAiChatCompletionsServer.enqueue(new MockResponse().setResponseCode(200).setBody(chatCompletionsResponse()));
            put(oldClusterId, chatCompletionsConfig(getUrl(openAiChatCompletionsServer)), testTaskType);

            var configs = (List<Map<String, Object>>) get(testTaskType, oldClusterId).get(old_cluster_endpoint_identifier);
            assertThat(configs, hasSize(1));

            assertCompletionInference(oldClusterId);
        } else if (isMixedCluster()) {
            List<Map<String, Object>> configs = new LinkedList<>();
            var request = get(testTaskType, oldClusterId);
            configs.addAll((Collection<? extends Map<String, Object>>) request.getOrDefault("endpoints", List.of()));
            if (oldClusterHasFeature(MODELS_RENAMED_TO_ENDPOINTS_FEATURE) == false) {
                configs.addAll((List<Map<String, Object>>) request.getOrDefault(old_cluster_endpoint_identifier, List.of()));
                // in version 8.15, there was a breaking change where "models" was renamed to "endpoints"
            }
            assertEquals("openai", configs.get(0).get("service"));
            var serviceSettings = (Map<String, Object>) configs.get(0).get("service_settings");
            assertThat(serviceSettings, hasEntry("model_id", "gpt-4"));
            var taskSettings = (Map<String, Object>) configs.get(0).get("task_settings");
            assertThat(taskSettings, anyOf(nullValue(), anEmptyMap()));

            assertCompletionInference(oldClusterId);
        } else if (isUpgradedCluster()) {
            // check old cluster model
            var configs = (List<Map<String, Object>>) get(testTaskType, oldClusterId).get("endpoints");
            var serviceSettings = (Map<String, Object>) configs.get(0).get("service_settings");
            assertThat(serviceSettings, hasEntry("model_id", "gpt-4"));
            var taskSettings = (Map<String, Object>) configs.get(0).get("task_settings");
            assertThat(taskSettings, anyOf(nullValue(), anEmptyMap()));

            assertCompletionInference(oldClusterId);

            openAiChatCompletionsServer.enqueue(new MockResponse().setResponseCode(200).setBody(chatCompletionsResponse()));
            put(upgradedClusterId, chatCompletionsConfig(getUrl(openAiChatCompletionsServer)), testTaskType);
            configs = (List<Map<String, Object>>) get(testTaskType, upgradedClusterId).get("endpoints");
            assertThat(configs, hasSize(1));

            // Inference on the new config
            assertCompletionInference(upgradedClusterId);

            delete(oldClusterId);
            delete(upgradedClusterId);
        }
    }

    void assertCompletionInference(String inferenceId) throws IOException {
        openAiChatCompletionsServer.enqueue(new MockResponse().setResponseCode(200).setBody(chatCompletionsResponse()));
        var inferenceMap = inference(inferenceId, TaskType.COMPLETION, "some text");
        assertThat(inferenceMap.entrySet(), not(empty()));
    }

    private String oldClusterVersionCompatibleEmbeddingConfig() {
        if (oldClusterHasFeature(OPEN_AI_EMBEDDINGS_MODEL_SETTING_MOVED_TEST_FEATURE) == false) {
            return embeddingConfigWithModelInTaskSettings(getUrl(openAiEmbeddingsServer));
        } else {
            return embeddingConfigWithModelInServiceSettings(getUrl(openAiEmbeddingsServer));
        }
    }

    private String embeddingConfigWithModelInTaskSettings(String url) {
        return Strings.format("""
            {
                "service": "openai",
                "service_settings": {
                    "api_key": "XXXX",
                    "url": "%s"
                },
                "task_settings": {
                   "model": "text-embedding-ada-002"
                }
            }
            """, url);
    }

    static String embeddingConfigWithModelInServiceSettings(String url) {
        return Strings.format("""
            {
                "service": "openai",
                "service_settings": {
                    "api_key": "XXXX",
                    "url": "%s",
                    "model_id": "text-embedding-ada-002"
                }
            }
            """, url);
    }

    private String chatCompletionsConfig(String url) {
        return Strings.format("""
            {
                "service": "openai",
                "service_settings": {
                    "api_key": "XXXX",
                    "url": "%s",
                    "model_id": "gpt-4"
                }
            }
            """, url);
    }

    static String embeddingResponse() {
        return """
            {
              "object": "list",
              "data": [
                  {
                      "object": "embedding",
                      "index": 0,
                      "embedding": [
                          0.0123,
                          -0.0123
                      ]
                  }
              ],
              "model": "text-embedding-ada-002",
              "usage": {
                  "prompt_tokens": 8,
                  "total_tokens": 8
              }
            }
            """;
    }

    private String chatCompletionsResponse() {
        return """
            {
              "id": "some-id",
              "object": "chat.completion",
              "created": 1705397787,
              "model": "gpt-3.5-turbo-0613",
              "choices": [
                {
                  "index": 0,
                  "message": {
                    "role": "assistant",
                    "content": "some content"
                  },
                  "logprobs": null,
                  "finish_reason": "stop"
                }
              ],
              "usage": {
                "prompt_tokens": 46,
                "completion_tokens": 39,
                "total_tokens": 85
              },
              "system_fingerprint": null
            }
            """;
    }
}
