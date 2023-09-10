/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

/**
 * Test suite for the various combinators on {@link ActionListener}, verifying that the various optimisations which apply to stacks of the
 * combinators don't affect their semantics.
 */
public class ActionListenerStackTests extends ESTestCase {

    private Throwable innerOnResponseResult;
    private Throwable innerOnFailureResult;

    private boolean innerOnResponseCalled;
    private boolean innerOnFailureCalled;

    private int exceptionIndex = 0;
    private Exception outerException;

    private Integer expectedInnerResponseFromOuterOnResponse = 1;
    private String expectedInnerExceptionMessageFromOuterOnResponse = null;
    private String expectedInnerExceptionMessageFromOuterOnFailure = outerException.getMessage();
    private String expectedInnerExceptionMessage;

    private Throwable expectedOuterOnResponseResult;
    private Throwable expectedOuterOnFailureResult;

    private boolean outerOnResponseCallsInnerOnResponse = true;
    private boolean outerOnResponseCallsInnerOnFailure = false;
    private boolean outerOnFailureCallsInnerOnFailure = true;

    private ActionListener<Integer> listener = new InnerListener();

    @Before
    public void resetStack() {
        if (randomBoolean()) {
            innerOnResponseResult = expectedOuterOnResponseResult = null;
        } else {
            innerOnResponseResult = expectedOuterOnResponseResult = new CallingOnResponseThrowsException();
        }

        if (randomBoolean()) {
            innerOnFailureResult = expectedOuterOnFailureResult = null;
        } else {
            innerOnFailureResult = expectedOuterOnFailureResult = new CallingOnFailureThrowsException();
        }

        exceptionIndex = 0;
        outerException = newException();
        listener = new InnerListener();
    }

    private Exception newException() {
        return new ElasticsearchException(Integer.toString(exceptionIndex++));
    }

    public void testActionListenerStack() {
        for (int i = between(0, 4); i > 0; i--) {
            addWrapper();
        }

        logger.info("resulting listener: {}", listener);

        if (randomBoolean()) {
            logger.info("completing with onResponse(1)");
            expectedInnerExceptionMessage = expectedInnerExceptionMessageFromOuterOnResponse;
            final Runnable listenerCompleter = () -> listener.onResponse(1);
            if (expectedOuterOnResponseResult instanceof CallingOnResponseThrowsException) {
                expectThrows(CallingOnResponseThrowsException.class, listenerCompleter::run);
            } else if (expectedOuterOnResponseResult instanceof CallingOnResponseIsAnError) {
                expectThrows(AssertionError.class, listenerCompleter::run);
            } else if (expectedOuterOnResponseResult == null) {
                listenerCompleter.run();
            } else {
                fail("impossible");
            }

            assertEquals(outerOnResponseCallsInnerOnResponse, innerOnResponseCalled);
            assertEquals(outerOnResponseCallsInnerOnFailure, innerOnFailureCalled);
        } else {
            logger.info("completing with onFailure(outerException)");
            expectedInnerExceptionMessage = expectedInnerExceptionMessageFromOuterOnFailure;
            final Runnable listenerCompleter = () -> listener.onFailure(outerException);
            if (expectedOuterOnFailureResult instanceof CallingOnFailureThrowsException) {
                expectThrows(CallingOnFailureThrowsException.class, listenerCompleter::run);
            } else if (expectedOuterOnFailureResult instanceof CallingOnFailureIsAnError) {
                expectThrows(AssertionError.class, listenerCompleter::run);
            } else if (expectedOuterOnFailureResult == null) {
                listenerCompleter.run();
            } else {
                fail("impossible");
            }

            assertFalse(innerOnResponseCalled);
            assertEquals(outerOnFailureCallsInnerOnFailure, innerOnFailureCalled);
        }
    }

    private void addWrapper() {
        switch (between(0, 2)) {
            case 0 -> {
                // A no-op wrapper around the current listener, which disables optimisations without changing behaviour
                logger.info("wrapping trivially");
                listener = new DisablesOptimisations(listener);
            }
            case 1 -> {
                // A map() or equivalent wrapper
                final int factor = randomFrom(2, 3, 5, 7);
                final var wrapType = between(1, 3);
                logger.info("wrapping with map(*{}) type {}", factor, wrapType);
                listener = switch (wrapType) {
                    case 1 -> listener.map(i -> i * factor);
                    case 2 -> listener.safeMap(i -> i * factor);
                    case 3 -> listener.delegateFailure((l, i) -> l.onResponse(i * factor));
                    // TODO delegateFailureAndWrap
                    default -> throw new AssertionError("impossible");
                };

                expectedBehaviour = new ExpectedBehaviour(
                    onResponseForbidException(i -> i * factor),
                    onFailureForbidException(e -> e)
                );
            }
            case 2 -> {
                final var exception = newException();
                logger.info("wrapping with map(throwing({}))", exception.getMessage());
                listener = listener.map(i -> { throw exception; });

                expectedBehaviour = new ExpectedBehaviour(
                    onFailureForbidException(e -> exception),
                    onFailureForbidException(e -> e)
                );
            }
            default -> fail("impossible");
        }
    }

    @SuppressWarnings("NewClassNamingConvention")
    private static class CallingOnResponseThrowsException extends RuntimeException {}

    @SuppressWarnings("NewClassNamingConvention")
    private static class CallingOnFailureThrowsException extends RuntimeException {}

    @SuppressWarnings("NewClassNamingConvention")
    private static class CallingOnResponseIsAnError extends AssertionError {}

    @SuppressWarnings("NewClassNamingConvention")
    private static class CallingOnFailureIsAnError extends AssertionError {}

    @SuppressWarnings("NewClassNamingConvention")
    private static class DisablesOptimisations implements ActionListener<Integer> {

        private final ActionListener<Integer> delegate;

        DisablesOptimisations(ActionListener<Integer> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void onResponse(Integer integer) {
            delegate.onResponse(integer);
        }

        @Override
        public void onFailure(Exception e) {
            delegate.onFailure(e);
        }
    }

    private class InnerListener implements ActionListener<Integer> {
        @Override
        public void onResponse(Integer actualInnerResult) {
            assert innerOnResponseCalled == false;
            innerOnResponseCalled = true;
            assertEquals(expectedInnerResponseFromOuterOnResponse, actualInnerResult);
            completeHandler(innerOnResponseResult);
        }

        @Override
        public void onFailure(Exception e) {
            assert innerOnFailureCalled == false;
            innerOnFailureCalled = true;
            assertEquals(expectedInnerExceptionMessage, e.getMessage());
            completeHandler(innerOnFailureResult);
        }

        private static void completeHandler(Object result) {
            if (result instanceof RuntimeException runtimeException) {
                throw runtimeException;
            } else if (result instanceof Error error) {
                throw error;
            } else if (result != null) {
                fail("impossible");
            }
        }

        @Override
        public String toString() {
            return "InnerListener";
        }
    }
}
