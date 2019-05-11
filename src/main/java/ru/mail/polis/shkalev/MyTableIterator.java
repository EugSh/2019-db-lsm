package ru.mail.polis.shkalev;

import java.util.Collection;
import java.util.Iterator;

import org.jetbrains.annotations.NotNull;

import com.google.common.collect.Iterators;

import ru.mail.polis.Iters;

public final class MyTableIterator implements Iterator<Row> {
    private final Iterator<Row> iterator;
    private boolean hasNext;
    private Row next;

    private MyTableIterator(@NotNull final Iterator<Row> iterator) {
        this.iterator = iterator;
        hasNext = iterator.hasNext();
        if (hasNext) {
            next = iterator.next();
        }
    }

    public static MyTableIterator of(@NotNull final Iterator<Row> iterator) {
        return new MyTableIterator(iterator);
    }

    @Override
    public boolean hasNext() {
        return hasNext;
    }

    public Row peek() {
        assert hasNext;
        return next;
    }

    @Override
    public Row next() {
        assert hasNext;
        final Row row = next;
        hasNext = iterator.hasNext();
        if (hasNext) {
            next = iterator.next();
        }
        return row;
    }

    /**
     * get merge sorted, collapse equals, without dead row iterator.
     *
     * @param tableIterators collection MyTableIterator
     * @return Row Iterator
     */
    public static Iterator<Row> getActualRowIterator(@NotNull final Collection<MyTableIterator> tableIterators) {
        final Iterator<Row> mergingTableIterator = Iterators.mergeSorted(tableIterators, Row::compareTo);
        final Iterator<Row> collapsedIterator = Iters.collapseEquals(mergingTableIterator, Row::getKey);
        return Iterators.filter(collapsedIterator, row -> !row.isDead());
    }
}
