/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tasks;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public class ListTasksResponseTests extends ESTestCase {

    public void testEmptyToString() {
        assertEquals("""
            {
              "tasks" : [ ]
            }""", new ListTasksResponse(null, null, null).toString());
    }

    public void testNonEmptyToString() {
        TaskInfo info = new TaskInfo(
            new TaskId("node1", 1),
            "dummy-type",
            "dummy-action",
            "dummy-description",
            null,
            0,
            1,
            true,
            false,
            new TaskId("node1", 0),
            Collections.singletonMap("foo", "bar")
        );
        ListTasksResponse tasksResponse = new ListTasksResponse(singletonList(info), emptyList(), emptyList());
        assertEquals("""
            {
              "tasks" : [
                {
                  "node" : "node1",
                  "id" : 1,
                  "type" : "dummy-type",
                  "action" : "dummy-action",
                  "description" : "dummy-description",
                  "start_time" : "1970-01-01T00:00:00.000Z",
                  "start_time_in_millis" : 0,
                  "running_time" : "1nanos",
                  "running_time_in_nanos" : 1,
                  "cancellable" : true,
                  "cancelled" : false,
                  "parent_task_id" : "node1:0",
                  "headers" : {
                    "foo" : "bar"
                  }
                }
              ]
            }""", tasksResponse.toString());
    }

    private static List<TaskInfo> randomTasks() {
        List<TaskInfo> tasks = new ArrayList<>();
        for (int i = 0; i < randomInt(10); i++) {
            tasks.add(TaskInfoTests.randomTaskInfo());
        }
        return tasks;
    }

    public void testChunkedEncoding() {
        final var response = createTestInstanceWithFailures();
        AbstractChunkedSerializingTestCase.assertChunkCount(response.groupedByNone(), o -> response.getTasks().size() + 2);
        AbstractChunkedSerializingTestCase.assertChunkCount(response.groupedByParent(), o -> response.getTaskGroups().size() + 2);
        AbstractChunkedSerializingTestCase.assertChunkCount(
            response.groupedByNode(() -> DiscoveryNodes.EMPTY_NODES),
            o -> 2 + response.getPerNodeTasks().values().stream().mapToInt(entry -> 2 + entry.size()).sum()
        );
    }

    private static ListTasksResponse createTestInstanceWithFailures() {
        int numNodeFailures = randomIntBetween(0, 3);
        List<FailedNodeException> nodeFailures = new ArrayList<>(numNodeFailures);
        for (int i = 0; i < numNodeFailures; i++) {
            nodeFailures.add(new FailedNodeException(randomAlphaOfLength(5), "error message", new ConnectException()));
        }
        int numTaskFailures = randomIntBetween(0, 3);
        List<TaskOperationFailure> taskFailures = new ArrayList<>(numTaskFailures);
        for (int i = 0; i < numTaskFailures; i++) {
            taskFailures.add(new TaskOperationFailure(randomAlphaOfLength(5), randomLong(), new IllegalStateException()));
        }
        return new ListTasksResponse(randomTasks(), taskFailures, nodeFailures);
    }
}
