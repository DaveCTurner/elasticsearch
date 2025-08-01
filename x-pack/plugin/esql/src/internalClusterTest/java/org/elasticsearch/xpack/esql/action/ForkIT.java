/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.junit.Before;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.equalTo;

// @TestLogging(value = "org.elasticsearch.xpack.esql:TRACE,org.elasticsearch.compute:TRACE", reason = "debug")
public class ForkIT extends AbstractEsqlIntegTestCase {

    @Before
    public void setupIndex() {
        assumeTrue("requires FORK capability", EsqlCapabilities.Cap.FORK_V9.isEnabled());
        createAndPopulateIndices();
    }

    public void testSimple() {
        var query = """
            FROM test
            | WHERE id > 2
            | FORK
               ( WHERE content:"fox" )  // match operator
               ( WHERE content:"dog" )
            | KEEP id, _fork, content
            | SORT id, _fork
            """;
        testSimpleImpl(query);
    }

    public void testSimpleMatchFunction() {
        var query = """
            FROM test
            | WHERE id > 2
            | FORK
               ( WHERE match(content, "fox") )  // match function
               ( WHERE match(content, "dog") )
            | KEEP id, _fork, content
            | SORT id, _fork
            """;
        testSimpleImpl(query);
    }

    private void testSimpleImpl(String query) {
        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_fork", "content"));
            assertColumnTypes(resp.columns(), List.of("integer", "keyword", "text"));
            Iterable<Iterable<Object>> expectedValues = List.of(
                List.of(3, "fork2", "This dog is really brown"),
                List.of(4, "fork2", "The dog is brown but this document is very very long"),
                List.of(6, "fork1", "The quick brown fox jumps over the lazy dog"),
                List.of(6, "fork2", "The quick brown fox jumps over the lazy dog")
            );
            assertValues(resp.values(), expectedValues);
        }
    }

    public void testRow() {
        var query = """
            ROW a = [1, 2, 3, 4], b = 100
            | MV_EXPAND a
            | FORK (WHERE a % 2 == 1)
                   (WHERE a % 2 == 0)
            | SORT _fork, a
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("a", "b", "_fork"));
            assertColumnTypes(resp.columns(), List.of("integer", "integer", "keyword"));

            Iterable<Iterable<Object>> expectedValues = List.of(
                List.of(1, 100, "fork1"),
                List.of(3, 100, "fork1"),
                List.of(2, 100, "fork2"),
                List.of(4, 100, "fork2")
            );
            assertValues(resp.values(), expectedValues);
        }
    }

    public void testSortAndLimitInFirstSubQuery() {
        var query = """
            FROM test
            | WHERE id > 0
            | FORK
               ( WHERE content:"fox" | SORT id DESC | LIMIT 1 )
               ( WHERE content:"dog" )
            | KEEP id, _fork, content
            | SORT id, _fork
            """;
        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_fork", "content"));
            assertColumnTypes(resp.columns(), List.of("integer", "keyword", "text"));
            Iterable<Iterable<Object>> expectedValues = List.of(
                List.of(2, "fork2", "This is a brown dog"),
                List.of(3, "fork2", "This dog is really brown"),
                List.of(4, "fork2", "The dog is brown but this document is very very long"),
                List.of(6, "fork1", "The quick brown fox jumps over the lazy dog"),
                List.of(6, "fork2", "The quick brown fox jumps over the lazy dog")
            );
            assertValues(resp.values(), expectedValues);
        }
    }

    public void testSortAndLimitInFirstSubQueryASC() {
        var query = """
            FROM test
            | WHERE id > 0
            | FORK
               ( WHERE content:"fox" | SORT id ASC | LIMIT 1 )
               ( WHERE content:"dog" )
            | KEEP id, _fork, content
            | SORT id, _fork
            """;
        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_fork", "content"));
            assertColumnTypes(resp.columns(), List.of("integer", "keyword", "text"));
            Iterable<Iterable<Object>> expectedValues = List.of(
                List.of(1, "fork1", "This is a brown fox"),
                List.of(2, "fork2", "This is a brown dog"),
                List.of(3, "fork2", "This dog is really brown"),
                List.of(4, "fork2", "The dog is brown but this document is very very long"),
                List.of(6, "fork2", "The quick brown fox jumps over the lazy dog")
            );
            assertValues(resp.values(), expectedValues);
        }
    }

    public void testSortAndLimitInSecondSubQuery() {
        var query = """
            FROM test
            | WHERE id > 2
            | FORK
               ( WHERE content:"fox" )
               ( WHERE content:"dog" | SORT id DESC | LIMIT 2 )
            | KEEP _fork, id, content
            | SORT _fork, id
            """;
        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("_fork", "id", "content"));
            assertColumnTypes(resp.columns(), List.of("keyword", "integer", "text"));
            Iterable<Iterable<Object>> expectedValues = List.of(
                List.of("fork1", 6, "The quick brown fox jumps over the lazy dog"),
                List.of("fork2", 4, "The dog is brown but this document is very very long"),
                List.of("fork2", 6, "The quick brown fox jumps over the lazy dog")
            );
            assertValues(resp.values(), expectedValues);
        }
    }

    public void testSortAndLimitInBothSubQueries() {
        var query = """
            FROM test
            | WHERE id > 0
            | FORK
               ( WHERE content:"fox" | SORT id | LIMIT 1 )
               ( WHERE content:"dog" | SORT id | LIMIT 1 )
            | KEEP id, _fork, content
            | SORT id, _fork
            """;
        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_fork", "content"));
            assertColumnTypes(resp.columns(), List.of("integer", "keyword", "text"));
            Iterable<Iterable<Object>> expectedValues = List.of(
                List.of(1, "fork1", "This is a brown fox"),
                List.of(2, "fork2", "This is a brown dog")
            );
            assertValues(resp.values(), expectedValues);
        }
    }

    public void testWhereWhere() {
        var query = """
            FROM test
            | FORK
               ( WHERE id < 2 | WHERE content:"fox" )
               ( WHERE id > 2 | WHERE content:"dog" )
            | SORT _fork, id
            | KEEP _fork, id, content
            """;
        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("_fork", "id", "content"));
            assertColumnTypes(resp.columns(), List.of("keyword", "integer", "text"));
            Iterable<Iterable<Object>> expectedValues = List.of(
                List.of("fork1", 1, "This is a brown fox"),
                List.of("fork2", 3, "This dog is really brown"),
                List.of("fork2", 4, "The dog is brown but this document is very very long"),
                List.of("fork2", 6, "The quick brown fox jumps over the lazy dog")
            );
            assertValues(resp.values(), expectedValues);
        }
    }

    public void testWhereSort() {
        var query = """
            FROM test
            | FORK
               ( WHERE content:"fox" | SORT id )
               ( WHERE content:"dog" | SORT id )
            | SORT _fork, id
            | KEEP _fork, id, content
            """;
        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("_fork", "id", "content"));
            assertColumnTypes(resp.columns(), List.of("keyword", "integer", "text"));
            Iterable<Iterable<Object>> expectedValues = List.of(
                List.of("fork1", 1, "This is a brown fox"),
                List.of("fork1", 6, "The quick brown fox jumps over the lazy dog"),
                List.of("fork2", 2, "This is a brown dog"),
                List.of("fork2", 3, "This dog is really brown"),
                List.of("fork2", 4, "The dog is brown but this document is very very long"),
                List.of("fork2", 6, "The quick brown fox jumps over the lazy dog")
            );
            assertValues(resp.values(), expectedValues);
        }
    }

    public void testWhereSortOnlyInFork() {
        var queryWithMatchOperator = """
            FROM test
            | FORK
               ( WHERE content:"fox" | SORT id )
               ( WHERE content:"dog" | SORT id )
            | KEEP _fork, id, content
            | SORT _fork, id
            """;
        var queryWithMatchFunction = """
            FROM test
            | FORK
               ( WHERE match(content, "fox") | SORT id )
               ( WHERE match(content, "dog") | SORT id )
            | KEEP _fork, id, content
            | SORT _fork, id
            """;
        for (var query : List.of(queryWithMatchOperator, queryWithMatchFunction)) {
            try (var resp = run(query)) {
                assertColumnNames(resp.columns(), List.of("_fork", "id", "content"));
                assertColumnTypes(resp.columns(), List.of("keyword", "integer", "text"));
                Iterable<Iterable<Object>> expectedValues = List.of(
                    List.of("fork1", 1, "This is a brown fox"),
                    List.of("fork1", 6, "The quick brown fox jumps over the lazy dog"),
                    List.of("fork2", 2, "This is a brown dog"),
                    List.of("fork2", 3, "This dog is really brown"),
                    List.of("fork2", 4, "The dog is brown but this document is very very long"),
                    List.of("fork2", 6, "The quick brown fox jumps over the lazy dog")
                );
                assertValues(resp.values(), expectedValues);
            }
        }
    }

    public void testSortAndLimitOnlyInSecondSubQuery() {
        var query = """
            FROM test
            | FORK
               ( WHERE content:"fox" )
               ( SORT id | LIMIT 3 )
            | SORT _fork, id
            | KEEP _fork, id, content
            """;
        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("_fork", "id", "content"));
            assertColumnTypes(resp.columns(), List.of("keyword", "integer", "text"));
            Iterable<Iterable<Object>> expectedValues = List.of(
                List.of("fork1", 1, "This is a brown fox"),
                List.of("fork1", 6, "The quick brown fox jumps over the lazy dog"),
                List.of("fork2", 1, "This is a brown fox"),
                List.of("fork2", 2, "This is a brown dog"),
                List.of("fork2", 3, "This dog is really brown")
            );
            assertValues(resp.values(), expectedValues);
        }
    }

    public void testLimitOnlyInFirstSubQuery() {
        var query = """
            FROM test
            | FORK
               ( LIMIT 100 )
               ( WHERE content:"fox" )
            | SORT _fork, id
            | KEEP _fork, id, content
            """;
        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("_fork", "id", "content"));
            assertColumnTypes(resp.columns(), List.of("keyword", "integer", "text"));
            Iterable<Iterable<Object>> expectedValues = List.of(
                List.of("fork1", 1, "This is a brown fox"),
                List.of("fork1", 2, "This is a brown dog"),
                List.of("fork1", 3, "This dog is really brown"),
                List.of("fork1", 4, "The dog is brown but this document is very very long"),
                List.of("fork1", 5, "There is also a white cat"),
                List.of("fork1", 6, "The quick brown fox jumps over the lazy dog"),
                List.of("fork2", 1, "This is a brown fox"),
                List.of("fork2", 6, "The quick brown fox jumps over the lazy dog")
            );
            assertValues(resp.values(), expectedValues);
        }
    }

    public void testLimitOnlyInSecondSubQuery() {
        var query = """
            FROM test
            | FORK
               ( WHERE content:"fox" )
               ( LIMIT 100 )
            | SORT _fork, id
            | KEEP _fork, id, content
            """;
        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("_fork", "id", "content"));
            assertColumnTypes(resp.columns(), List.of("keyword", "integer", "text"));
            Iterable<Iterable<Object>> expectedValues = List.of(
                List.of("fork1", 1, "This is a brown fox"),
                List.of("fork1", 6, "The quick brown fox jumps over the lazy dog"),
                List.of("fork2", 1, "This is a brown fox"),
                List.of("fork2", 2, "This is a brown dog"),
                List.of("fork2", 3, "This dog is really brown"),
                List.of("fork2", 4, "The dog is brown but this document is very very long"),
                List.of("fork2", 5, "There is also a white cat"),
                List.of("fork2", 6, "The quick brown fox jumps over the lazy dog")
            );
            assertValues(resp.values(), expectedValues);
        }
    }

    public void testKeepOnlyId() {
        var query = """
            FROM test METADATA _score
            | WHERE id > 2
            | FORK
               ( WHERE content:"fox" )
               ( WHERE content:"dog" )
            | KEEP id
            | SORT id
            """;
        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id"));
            assertColumnTypes(resp.columns(), List.of("integer"));
            Iterable<Iterable<Object>> expectedValues = List.of(List.of(3), List.of(4), List.of(6), List.of(6));
            assertValues(resp.values(), expectedValues);
        }
    }

    public void testScoringKeepAndSort() {
        var query = """
            FROM test METADATA _score
            | WHERE id > 2
            | FORK
               ( WHERE content:"fox" )
               ( WHERE content:"dog" )
            | KEEP id, content, _fork, _score
            | SORT id
            """;
        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "content", "_fork", "_score"));
            assertColumnTypes(resp.columns(), List.of("integer", "text", "keyword", "double"));
            assertThat(getValuesList(resp.values()).size(), equalTo(4)); // just assert that the expected number of results
        }
    }

    public void testThreeSubQueries() {
        var query = """
            FROM test
            | WHERE id > 2
            | FORK
               ( WHERE content:"fox" )
               ( WHERE content:"dog" )
               ( WHERE content:"cat" )
            | KEEP _fork, id, content
            | SORT _fork, id
            """;
        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("_fork", "id", "content"));
            assertColumnTypes(resp.columns(), List.of("keyword", "integer", "text"));
            Iterable<Iterable<Object>> expectedValues = List.of(
                List.of("fork1", 6, "The quick brown fox jumps over the lazy dog"),
                List.of("fork2", 3, "This dog is really brown"),
                List.of("fork2", 4, "The dog is brown but this document is very very long"),
                List.of("fork2", 6, "The quick brown fox jumps over the lazy dog"),
                List.of("fork3", 5, "There is also a white cat")
            );
            assertValues(resp.values(), expectedValues);
        }
    }

    public void testFiveSubQueries() {
        var query = """
            FROM test
            | FORK
               ( WHERE id == 6 )
               ( WHERE id == 2 )
               ( WHERE id == 5 )
               ( WHERE id == 1 )
               ( WHERE id == 3 )
            | SORT _fork, id
            | KEEP _fork, id, content
            """;
        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("_fork", "id", "content"));
            assertColumnTypes(resp.columns(), List.of("keyword", "integer", "text"));
            Iterable<Iterable<Object>> expectedValues = List.of(
                List.of("fork1", 6, "The quick brown fox jumps over the lazy dog"),
                List.of("fork2", 2, "This is a brown dog"),
                List.of("fork3", 5, "There is also a white cat"),
                List.of("fork4", 1, "This is a brown fox"),
                List.of("fork5", 3, "This dog is really brown")
            );
            assertValues(resp.values(), expectedValues);
        }
    }

    // Tests that sort order is preserved within each fork
    // subquery, without any subsequent overall stream sort
    public void testFourSubQueriesWithSortAndLimit() {
        var query = """
            FROM test
            | FORK
               ( WHERE id > 0 | SORT id DESC | LIMIT 2 )
               ( WHERE id > 1 | SORT id ASC  | LIMIT 3 )
               ( WHERE id < 3 | SORT id DESC | LIMIT 2 )
               ( WHERE id > 2 | SORT id ASC  | LIMIT 3 )
            | KEEP _fork, id, content
            """;
        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("_fork", "id", "content"));
            assertColumnTypes(resp.columns(), List.of("keyword", "integer", "text"));
            Iterable<Iterable<Object>> fork0 = List.of(
                List.of("fork1", 6, "The quick brown fox jumps over the lazy dog"),
                List.of("fork1", 5, "There is also a white cat")
            );
            Iterable<Iterable<Object>> fork1 = List.of(
                List.of("fork2", 2, "This is a brown dog"),
                List.of("fork2", 3, "This dog is really brown"),
                List.of("fork2", 4, "The dog is brown but this document is very very long")
            );
            Iterable<Iterable<Object>> fork2 = List.of(
                List.of("fork3", 2, "This is a brown dog"),
                List.of("fork3", 1, "This is a brown fox")
            );
            Iterable<Iterable<Object>> fork3 = List.of(
                List.of("fork4", 3, "This dog is really brown"),
                List.of("fork4", 4, "The dog is brown but this document is very very long"),
                List.of("fork4", 5, "There is also a white cat")
            );
            assertValues(valuesFilter(resp.values(), row -> row.next().equals("fork1")), fork0);
            assertValues(valuesFilter(resp.values(), row -> row.next().equals("fork2")), fork1);
            assertValues(valuesFilter(resp.values(), row -> row.next().equals("fork3")), fork2);
            assertValues(valuesFilter(resp.values(), row -> row.next().equals("fork4")), fork3);
            assertThat(getValuesList(resp.values()).size(), equalTo(10));
        }
    }

    public void testSubqueryWithoutResults() {
        var query = """
            FROM test
            | WHERE id > 2
            | FORK
               ( WHERE content:"rabbit" )
               ( WHERE content:"dog" )
               ( WHERE content:"cat" )
            | KEEP _fork, id, content
            | SORT _fork, id
            """;
        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("_fork", "id", "content"));
            assertColumnTypes(resp.columns(), List.of("keyword", "integer", "text"));
            Iterable<Iterable<Object>> expectedValues = List.of(
                List.of("fork2", 3, "This dog is really brown"),
                List.of("fork2", 4, "The dog is brown but this document is very very long"),
                List.of("fork2", 6, "The quick brown fox jumps over the lazy dog"),
                List.of("fork3", 5, "There is also a white cat")
            );
            assertValues(resp.values(), expectedValues);
        }
    }

    public void testAllSubQueriesWithoutResults() {
        var query = """
            FROM test
            | FORK
               ( WHERE content:"rabbit" )
               ( WHERE content:"lion" )
               ( WHERE content:"tiger" )
            | KEEP _fork, id, content
            | SORT _fork, id
            """;
        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("_fork", "id", "content"));
            assertColumnTypes(resp.columns(), List.of("keyword", "integer", "text"));
            Iterable<Iterable<Object>> empty = List.of();
            assertValues(resp.values(), empty);
        }
    }

    public void testSubqueryWithoutLimitOnly() {   // this should
        var query = """
            FROM test
            | FORK
               ( LIMIT 0 )  // verify optimizes away
               ( WHERE content:"cat" )
            | KEEP _fork, id, content
            | SORT _fork, id
            """;
        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("_fork", "id", "content"));
            assertColumnTypes(resp.columns(), List.of("keyword", "integer", "text"));
            Iterable<Iterable<Object>> expectedValues = List.of(List.of("fork2", 5, "There is also a white cat"));
            assertValues(resp.values(), expectedValues);
        }
    }

    public void testWithEvalSimple() {
        var query = """
                FROM test
                | WHERE content:"cat"
                | FORK ( EVAL a = 1 )
                       ( EVAL a = 2 )
                | KEEP a, _fork, id, content
                | SORT _fork
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("a", "_fork", "id", "content"));

            Iterable<Iterable<Object>> expectedValues = List.of(
                List.of(1, "fork1", 5, "There is also a white cat"),
                List.of(2, "fork2", 5, "There is also a white cat")
            );
            assertValues(resp.values(), expectedValues);
        }
    }

    public void testWithEvalDifferentOutputs() {
        var query = """
                FROM test
                | WHERE id == 2
                | FORK ( EVAL a = 1 )
                       ( EVAL b = 2 )
                | KEEP a, b, _fork
                | SORT _fork, a
            """;
        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("a", "b", "_fork"));
            Iterable<Iterable<Object>> expectedValues = List.of(
                Arrays.stream(new Object[] { 1, null, "fork1" }).toList(),
                Arrays.stream(new Object[] { null, 2, "fork2" }).toList()
            );
            assertValues(resp.values(), expectedValues);
        }
    }

    public void testWithStatsSimple() {
        var query = """
                FROM test
                | FORK (STATS x=COUNT(*), y=MV_SORT(VALUES(id)))
                       (WHERE id == 2)
                | KEEP _fork, x, y, id
                | SORT _fork, id
            """;
        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("_fork", "x", "y", "id"));
            Iterable<Iterable<Object>> expectedValues = List.of(
                Arrays.stream(new Object[] { "fork1", 6L, List.of(1, 2, 3, 4, 5, 6), null }).toList(),
                Arrays.stream(new Object[] { "fork2", null, null, 2 }).toList()
            );
            assertValues(resp.values(), expectedValues);
        }
    }

    public void testWithStatsAfterFork() {
        var query = """
                FROM test
                | FORK ( WHERE content:"fox" | EVAL a = 1)
                       ( WHERE content:"cat" | EVAL b = 2 )
                       ( WHERE content:"dog" | EVAL c = 3 )
                | STATS c = count(*)
            """;
        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("c"));
            assertColumnTypes(resp.columns(), List.of("long"));
            Iterable<Iterable<Object>> expectedValues = List.of(List.of(7L));
            assertValues(resp.values(), expectedValues);
        }
    }

    public void testWithStatsWithWhereAfterFork() {
        var query = """
                FROM test
                | FORK ( WHERE content:"fox" | EVAL a = 1)
                       ( WHERE content:"cat" | EVAL b = 2 )
                       ( WHERE content:"dog" | EVAL c = 3 )
                | STATS c = count(*) WHERE _fork == "fork1"
            """;
        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("c"));
            assertColumnTypes(resp.columns(), List.of("long"));

            Iterable<Iterable<Object>> expectedValues = List.of(List.of(2L));
            assertValues(resp.values(), expectedValues);
        }
    }

    public void testWithConditionOnForkField() {
        var query = """
                FROM test
                | FORK ( WHERE content:"fox" | EVAL a = 1)
                       ( WHERE content:"cat" | EVAL b = 2 )
                       ( WHERE content:"dog" | EVAL c = 3 )
                | WHERE _fork == "fork2"
                | KEEP _fork, id, content, a, b, c
                | SORT _fork
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("_fork", "id", "content", "a", "b", "c"));

            Iterable<Iterable<Object>> expectedValues = List.of(
                Arrays.stream(new Object[] { "fork2", 5, "There is also a white cat", null, 2, null }).toList()
            );
            assertValues(resp.values(), expectedValues);
        }
    }

    public void testWithFilteringOnConstantColumn() {
        var query = """
                FROM test
                | FORK ( WHERE content:"fox" | EVAL a = 1)
                       ( WHERE content:"cat" | EVAL a = 2 )
                       ( WHERE content:"dog" | EVAL a = 3 )
                | WHERE a == 3
                | KEEP _fork, id, content, a
                | SORT id
                | LIMIT 3
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("_fork", "id", "content", "a"));

            Iterable<Iterable<Object>> expectedValues = List.of(
                List.of("fork3", 2, "This is a brown dog", 3),
                List.of("fork3", 3, "This dog is really brown", 3),
                List.of("fork3", 4, "The dog is brown but this document is very very long", 3)
            );
            assertValues(resp.values(), expectedValues);
        }
    }

    public void testWithLookUpJoinBeforeFork() {
        var query = """
                FROM test
                | LOOKUP JOIN test-lookup ON id
                | FORK (WHERE id == 2 OR id == 3)
                       (WHERE id == 1 OR id == 2)
                | SORT _fork, id
            """;
        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("content", "id", "animal", "_fork"));
            Iterable<Iterable<Object>> expectedValues = List.of(
                List.of("This is a brown dog", 2, "dog", "fork1"),
                List.of("This dog is really brown", 3, "dog", "fork1"),
                List.of("This is a brown fox", 1, "fox", "fork2"),
                List.of("This is a brown dog", 2, "dog", "fork2")
            );
            assertValues(resp.values(), expectedValues);
        }
    }

    public void testWithLookUpAfterFork() {
        var query = """
                FROM test
                | FORK (WHERE id == 2 OR id == 3)
                       (WHERE id == 1 OR id == 2)
                | LOOKUP JOIN test-lookup ON id
                | SORT _fork, id
            """;
        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("content", "id", "_fork", "animal"));
            Iterable<Iterable<Object>> expectedValues = List.of(
                List.of("This is a brown dog", 2, "fork1", "dog"),
                List.of("This dog is really brown", 3, "fork1", "dog"),
                List.of("This is a brown fox", 1, "fork2", "fox"),
                List.of("This is a brown dog", 2, "fork2", "dog")
            );
            assertValues(resp.values(), expectedValues);
        }
    }

    public void testWithUnionTypesBeforeFork() {
        var query = """
                FROM test,test-other
                | EVAL x = id::keyword
                | EVAL id = id::keyword
                | EVAL content = content::keyword
                | FORK (WHERE x == "2")
                       (WHERE x == "1")
                | SORT _fork, x, content
                | KEEP content, id, x, _fork
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("content", "id", "x", "_fork"));
            Iterable<Iterable<Object>> expectedValues = List.of(
                List.of("This is a brown dog", "2", "2", "fork1"),
                List.of("This is a brown dog", "2", "2", "fork1"),
                List.of("This is a brown fox", "1", "1", "fork2"),
                List.of("This is a brown fox", "1", "1", "fork2")
            );
            assertValues(resp.values(), expectedValues);
        }
    }

    public void testWithUnionTypesInBranches() {
        var query = """
                FROM test,test-other
                | EVAL content = content::keyword
                | FORK (EVAL x = id::keyword |  WHERE x == "2" | EVAL id = x::integer)
                       (EVAL x = "a" | WHERE id::keyword == "1" | EVAL id = id::integer)
                | SORT _fork, x
                | KEEP content, id, x, _fork
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("content", "id", "x", "_fork"));
            Iterable<Iterable<Object>> expectedValues = List.of(
                List.of("This is a brown dog", 2, "2", "fork1"),
                List.of("This is a brown dog", 2, "2", "fork1"),
                List.of("This is a brown fox", 1, "a", "fork2"),
                List.of("This is a brown fox", 1, "a", "fork2")
            );
            assertValues(resp.values(), expectedValues);
        }
    }

    public void testWithDrop() {
        var query = """
            FROM test
            | WHERE id > 2
            | FORK
               ( WHERE content:"fox" | DROP content)
               ( WHERE content:"dog" | DROP content)
            | KEEP id, _fork
            | SORT id, _fork
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_fork"));
            assertColumnTypes(resp.columns(), List.of("integer", "keyword"));
            Iterable<Iterable<Object>> expectedValues = List.of(
                List.of(3, "fork2"),
                List.of(4, "fork2"),
                List.of(6, "fork1"),
                List.of(6, "fork2")
            );
            assertValues(resp.values(), expectedValues);
        }
    }

    public void testWithKeep() {
        var query = """
            FROM test
            | WHERE id > 2
            | FORK
               ( WHERE content:"fox" | KEEP id)
               ( WHERE content:"dog" | KEEP id)
            | SORT id, _fork
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("id", "_fork"));
            assertColumnTypes(resp.columns(), List.of("integer", "keyword"));
            Iterable<Iterable<Object>> expectedValues = List.of(
                List.of(3, "fork2"),
                List.of(4, "fork2"),
                List.of(6, "fork1"),
                List.of(6, "fork2")
            );
            assertValues(resp.values(), expectedValues);
        }
    }

    public void testWithUnsupportedFieldsWithSameBranches() {
        var query = """
            FROM test-other
            | FORK
               ( WHERE id == "3")
               ( WHERE id == "2" )
            | SORT _fork
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("content", "embedding", "id", "_fork"));
            assertColumnTypes(resp.columns(), List.of("keyword", "unsupported", "keyword", "keyword"));
            Iterable<Iterable<Object>> expectedValues = List.of(
                Arrays.stream(new Object[] { "This dog is really brown", null, "3", "fork1" }).toList(),
                Arrays.stream(new Object[] { "This is a brown dog", null, "2", "fork2" }).toList()
            );
            assertValues(resp.values(), expectedValues);
        }
    }

    public void testWithUnsupportedFieldsWithDifferentBranches() {
        var query = """
            FROM test-other
            | FORK
               ( STATS x = count(*))
               ( WHERE id == "2" )
            | SORT _fork
            """;

        try (var resp = run(query)) {
            assertColumnNames(resp.columns(), List.of("x", "_fork", "content", "embedding", "id"));
            assertColumnTypes(resp.columns(), List.of("long", "keyword", "keyword", "unsupported", "keyword"));
            Iterable<Iterable<Object>> expectedValues = List.of(
                Arrays.stream(new Object[] { 3L, "fork1", null, null, null }).toList(),
                Arrays.stream(new Object[] { null, "fork2", "This is a brown dog", null, "2" }).toList()
            );
            assertValues(resp.values(), expectedValues);
        }
    }

    public void testWithUnsupportedFieldsAndConflicts() {
        var firstQuery = """
            FROM test-other
            | FORK
               ( STATS embedding = count(*))
               ( WHERE id == "2" )
            | SORT _fork
            """;
        var e = expectThrows(VerificationException.class, () -> run(firstQuery));
        assertTrue(e.getMessage().contains("Column [embedding] has conflicting data types"));

        var secondQuery = """
            FROM test-other
            | FORK
               ( WHERE id == "2" )
               ( STATS embedding = count(*))
            | SORT _fork
            """;
        e = expectThrows(VerificationException.class, () -> run(secondQuery));
        assertTrue(e.getMessage().contains("Column [embedding] has conflicting data types"));

        var thirdQuery = """
            FROM test-other
            | FORK
               ( WHERE id == "2" )
               ( WHERE id == "3" )
               ( STATS embedding = count(*))
            | SORT _fork
            """;
        e = expectThrows(VerificationException.class, () -> run(thirdQuery));
        assertTrue(e.getMessage().contains("Column [embedding] has conflicting data types"));
    }

    public void testValidationsAfterFork() {
        var firstQuery = """
                FROM test*
                | FORK ( WHERE true )
                       ( WHERE true )
                | DROP _fork
                | STATS a = count_distinct(embedding)
            """;

        var e = expectThrows(VerificationException.class, () -> run(firstQuery));
        assertTrue(
            e.getMessage().contains("[count_distinct(embedding)] must be [any exact type except unsigned_long, _source, or counter types]")
        );

        var secondQuery = """
                FROM test*
                | FORK ( WHERE true )
                       ( WHERE true )
                | DROP _fork
                | EVAL a = substring(1, 2, 3)
            """;

        e = expectThrows(VerificationException.class, () -> run(secondQuery));
        assertTrue(e.getMessage().contains("first argument of [substring(1, 2, 3)] must be [string], found value [1] type [integer]"));

        var thirdQuery = """
                FROM test*
                | FORK ( WHERE true )
                       ( WHERE true )
                | DROP _fork
                | EVAL a = b + 2
            """;

        e = expectThrows(VerificationException.class, () -> run(thirdQuery));
        assertTrue(e.getMessage().contains("Unknown column [b]"));
    }

    public void testWithEvalWithConflictingTypes() {
        var query = """
                FROM test
                | FORK ( EVAL a = 1 )
                       ( EVAL a = "aaaa" )
                | KEEP a, _fork
            """;

        var e = expectThrows(VerificationException.class, () -> run(query));
        assertTrue(e.getMessage().contains("Column [a] has conflicting data types"));
    }

    public void testSubqueryWithUnknownField() {
        var query = """
            FROM test
            | FORK
               ( WHERE foo:"dog" )   // unknown field foo
               ( WHERE content:"cat" )
            | KEEP _fork, id, content
            | SORT _fork, id
            """;
        var e = expectThrows(VerificationException.class, () -> run(query));
        assertTrue(e.getMessage().contains("Unknown column [foo]"));
    }

    public void testSubqueryWithUnknownFieldMatchFunction() {
        var query = """
            FROM test
            | FORK
               ( WHERE match(bar, "dog") )   // unknown field bar
               ( WHERE content:"cat" )
            | KEEP _fork, id, content
            | SORT _fork, id
            """;
        var e = expectThrows(VerificationException.class, () -> run(query));
        assertTrue(e.getMessage().contains("Unknown column [bar]"));
    }

    public void testSubqueryWithUnknownFieldInThirdBranch() {
        var query = """
            FROM test
            | FORK
               ( WHERE content:"cat" )
               ( WHERE content:"dog" )
               ( WHERE fubar:"fox" )  // unknown fubar
               ( WHERE content:"rabbit" )
            | KEEP _fork, id, content
            """;
        var e = expectThrows(VerificationException.class, () -> run(query));
        assertTrue(e.getMessage().contains("Unknown column [fubar]"));
    }

    public void testSubqueryWithUnknownFieldInSort() {
        var query = """
            FROM test
            | FORK
               ( WHERE content:"dog" | sort baz)   // unknown field baz
               ( WHERE content:"cat" )
            | KEEP _fork, id, content
            | SORT _fork, id
            """;
        var e = expectThrows(VerificationException.class, () -> run(query));
        assertTrue(e.getMessage().contains("Unknown column [baz]"));

        var queryTwo = """
            FROM test
            | FORK
               ( WHERE content:"dog" )
               ( WHERE content:"cat" | sort bar)  // unknown field bar
            | KEEP _fork, id, content
            | SORT _fork, id
            """;
        e = expectThrows(VerificationException.class, () -> run(queryTwo));
        assertTrue(e.getMessage().contains("Unknown column [bar]"));
    }

    public void testSubqueryWithUnknownFieldInEval() {
        var query = """
            FROM test
            | FORK
               ( EVAL x = baz + 1)
               ( WHERE content:"cat" )
            | KEEP _fork, id, content
            | SORT _fork, id
            """;
        var e = expectThrows(VerificationException.class, () -> run(query));
        assertTrue(e.getMessage().contains("Unknown column [baz]"));
    }

    public void testOneSubQuery() {
        var query = """
            FROM test
            | WHERE id > 2
            | FORK
               ( WHERE content:"fox" )
            """;
        var e = expectThrows(ParsingException.class, () -> run(query));
        assertTrue(e.getMessage().contains("Fork requires at least 2 branches"));
    }

    public void testForkWithinFork() {
        var query = """
            FROM test
            | FORK ( FORK (WHERE true) (WHERE true) )
                   ( FORK (WHERE true) (WHERE true) )
            """;
        var e = expectThrows(VerificationException.class, () -> run(query));
        assertTrue(e.getMessage().contains("Only a single FORK command is supported, but found multiple"));
    }

    public void testProfile() {
        var query = """
            FROM test
            | FORK
               ( WHERE content:"fox" | SORT id )
               ( WHERE content:"dog" | SORT id )
            | SORT _fork, id
            | KEEP _fork, id, content
            """;

        EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest();

        request.pragmas(randomPragmas());
        request.query(query);
        request.profile(true);

        try (var resp = run(request)) {
            EsqlQueryResponse.Profile profile = resp.profile();
            assertNotNull(profile);

            assertEquals(
                Set.of("data", "main.final", "node_reduce", "subplan-0.final", "subplan-1.final"),
                profile.drivers().stream().map(DriverProfile::description).collect(Collectors.toSet())
            );
        }
    }

    public void testWithTooManySubqueries() {
        var query = """
            FROM test
            | FORK (WHERE true) (WHERE true) (WHERE true) (WHERE true) (WHERE true)
                   (WHERE true) (WHERE true) (WHERE true) (WHERE true)
            """;
        var e = expectThrows(ParsingException.class, () -> run(query));
        assertTrue(e.getMessage().contains("Fork supports up to 8 branches"));

    }

    private void createAndPopulateIndices() {
        var indexName = "test";
        var client = client().admin().indices();
        var createRequest = client.prepareCreate(indexName)
            .setSettings(Settings.builder().put("index.number_of_shards", 1))
            .setMapping("id", "type=integer", "content", "type=text");
        assertAcked(createRequest);
        client().prepareBulk()
            .add(new IndexRequest(indexName).id("1").source("id", 1, "content", "This is a brown fox"))
            .add(new IndexRequest(indexName).id("2").source("id", 2, "content", "This is a brown dog"))
            .add(new IndexRequest(indexName).id("3").source("id", 3, "content", "This dog is really brown"))
            .add(new IndexRequest(indexName).id("4").source("id", 4, "content", "The dog is brown but this document is very very long"))
            .add(new IndexRequest(indexName).id("5").source("id", 5, "content", "There is also a white cat"))
            .add(new IndexRequest(indexName).id("6").source("id", 6, "content", "The quick brown fox jumps over the lazy dog"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        ensureYellow(indexName);

        var lookupIndex = "test-lookup";
        createRequest = client.prepareCreate(lookupIndex)
            .setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.mode", "lookup"))
            .setMapping("id", "type=integer", "animal", "type=keyword");
        assertAcked(createRequest);

        client().prepareBulk()
            .add(new IndexRequest(lookupIndex).id("1").source("id", 1, "animal", "fox"))
            .add(new IndexRequest(lookupIndex).id("2").source("id", 2, "animal", "dog"))
            .add(new IndexRequest(lookupIndex).id("3").source("id", 3, "animal", "dog"))
            .add(new IndexRequest(lookupIndex).id("4").source("id", 4, "animal", "dog"))
            .add(new IndexRequest(lookupIndex).id("5").source("id", 5, "animal", "cat"))
            .add(new IndexRequest(lookupIndex).id("6").source("id", 6, "animal", List.of("fox", "dog")))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        ensureYellow(lookupIndex);

        var otherTestIndex = "test-other";

        createRequest = client.prepareCreate(otherTestIndex)
            .setSettings(Settings.builder().put("index.number_of_shards", 1))
            .setMapping("id", "type=keyword", "content", "type=keyword", "embedding", "type=sparse_vector");
        assertAcked(createRequest);
        client().prepareBulk()
            .add(
                new IndexRequest(otherTestIndex).id("1")
                    .source("id", "1", "content", "This is a brown fox", "embedding", Map.of("abc", 1.0))
            )
            .add(
                new IndexRequest(otherTestIndex).id("2")
                    .source("id", "2", "content", "This is a brown dog", "embedding", Map.of("def", 2.0))
            )
            .add(
                new IndexRequest(otherTestIndex).id("3")
                    .source("id", "3", "content", "This dog is really brown", "embedding", Map.of("ghi", 1.0))
            )
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        ensureYellow(indexName);
    }

    static Iterator<Iterator<Object>> valuesFilter(Iterator<Iterator<Object>> values, Predicate<Iterator<Object>> filter) {
        return getValuesList(values).stream().filter(row -> filter.test(row.iterator())).map(List::iterator).toList().iterator();
    }
}
