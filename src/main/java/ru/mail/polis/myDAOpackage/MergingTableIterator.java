package ru.mail.polis.myDAOpackage;

import java.util.Iterator;
import java.util.PriorityQueue;

import org.jetbrains.annotations.NotNull;


public class MergingTableIterator implements Iterator<Row> {
    private final PriorityQueue<MyTableIterator> queue = new PriorityQueue<>();

    public MergingTableIterator(@NotNull final MyTableIterator... iterators) {
        for (int i = 0; i < iterators.length; i++) {
            queue.add(iterators[i]);
        }
    }

    @Override
    public boolean hasNext() {
        return queue.size() > 0;
    }

    @Override
    public Row next() {
        assert hasNext();
        final MyTableIterator fileIterator = queue.remove();
        final Row row = fileIterator.next();
        if (fileIterator.hasNext()) {
            queue.add(fileIterator);
        }
        return row;
    }
}
