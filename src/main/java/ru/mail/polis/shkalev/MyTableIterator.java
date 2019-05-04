package ru.mail.polis.shkalev;

import java.util.Iterator;

import org.jetbrains.annotations.NotNull;

public class MyTableIterator implements Iterator<Row>, Comparable<MyTableIterator> {
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

    @Override
    public int compareTo(@NotNull final MyTableIterator o) {
        return next.compareTo(o.peek());
    }
}
