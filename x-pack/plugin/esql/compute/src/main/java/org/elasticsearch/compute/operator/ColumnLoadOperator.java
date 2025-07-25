/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;

/**
 * {@link Block#lookup Looks up} values from a provided {@link Block} and
 * merged them into each {@link Page}.
 */
public class ColumnLoadOperator extends AbstractPageMappingToIteratorOperator {
    public record Values(String name, Block block) {
        @Override
        public String toString() {
            return name + ":" + block.elementType();
        }
    }

    /**
     * Factory for {@link ColumnLoadOperator}. It's received {@link Block}s
     * are never closed, so we need to build them from a non-tracking factory.
     */
    public record Factory(Values values, int positionsOrd) implements OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            return new ColumnLoadOperator(values, positionsOrd);
        }

        @Override
        public String describe() {
            return "ColumnLoad[values=" + values + ", positions=" + positionsOrd + "]";
        }
    }

    private final Values values;
    private final int positionsOrd;

    public ColumnLoadOperator(Values values, int positionsOrd) {
        this.positionsOrd = positionsOrd;
        this.values = clone(values);
    }

    // FIXME: Since we don't have a thread-safe RefCounted for blocks/vectors, we have to clone the values block to avoid
    // data races of reference when sharing blocks/vectors across threads. Remove this when we have a thread-safe RefCounted
    // for blocks/vectors.
    static Values clone(Values values) {
        final Block block = values.block;
        try (var builder = block.elementType().newBlockBuilder(block.getPositionCount(), block.blockFactory())) {
            builder.copyFrom(block, 0, block.getPositionCount());
            return new Values(values.name, builder.build());
        }
    }

    /**
     * The target size of each loaded block.
     * TODO target the size more intelligently
     */
    static final ByteSizeValue TARGET_BLOCK_SIZE = ByteSizeValue.ofKb(10);

    @Override
    protected ReleasableIterator<Page> receive(Page page) {
        // TODO tracking is complex for values
        /*
         * values is likely shared across many threads so tracking it is complex.
         * Lookup will incRef it on the way in and decrement the ref on the way
         * out but it's not really clear what the right way to get all that thread
         * safe is. For now we can ignore this because we're not actually tracking
         * the memory of the block.
         */
        return appendBlocks(page, values.block.lookup(page.getBlock(positionsOrd), TARGET_BLOCK_SIZE));
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(values.block, super::close);
    }

    @Override
    public String toString() {
        return "ColumnLoad[values=" + values + ", positions=" + positionsOrd + "]";
    }
}
