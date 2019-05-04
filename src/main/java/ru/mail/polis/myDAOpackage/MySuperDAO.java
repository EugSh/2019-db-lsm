package ru.mail.polis.myDAOpackage;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.jetbrains.annotations.NotNull;

import com.google.common.collect.Iterators;

import ru.mail.polis.DAO;
import ru.mail.polis.Record;

public class MySuperDAO implements DAO {
    public static final ByteBuffer TOMBSTONE = ByteBuffer.allocate(0);
    public static final int ALIVE = 1;
    public static final int DEAD = 0;
    private static final int MODEL = Integer.parseInt(System.getProperty("sun.arch.data.model"));
    private static final int LINK_SIZE = MODEL == 64 ? 8 : 4;
    private static final int NUMBER_FIELDS_BYTEBUFFER = 7;
    private static final String PREFIX = "FT";
    private static final String SUFFIX = ".txt";
    private final NavigableMap<ByteBuffer, Row> memTable = new TreeMap<>();
    private final int maxHeap;
    private final File rootDir;
    private int currentFileIndex;
    private int currentHeap;

    public MySuperDAO(long maxHeap, File rootDir) throws IOException {
        assert maxHeap < Integer.MAX_VALUE;
        this.maxHeap = (int) maxHeap;
        this.rootDir = rootDir;
        currentHeap = 0;
        final Integer lastFileIndex = Files.walk(rootDir.toPath(), 1)
                .map(path -> path.getFileName().toString())
                .filter(str -> str.startsWith(PREFIX) && str.endsWith(SUFFIX))
                .map(str -> Integer.parseInt(str.substring(2, str.length() - 4)))
                .sorted(Comparator.reverseOrder())
                .findFirst()
                .orElse(0);
        currentFileIndex = lastFileIndex + 1;
    }


    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull ByteBuffer from) throws IOException {
        final List<String> fileNames = Files.walk(rootDir.toPath(), 1)
                .map(path -> path.getFileName().toString())
                .filter(str -> str.startsWith(PREFIX) && str.endsWith(SUFFIX))
                .collect(Collectors.toList());
        final List<MyTableIterator> tableIterators = new LinkedList<>();
        for (String fileName : fileNames) {
            //final FileTableNotWin fileTable = new FileTableNotWin(new File(rootDir, fileName));
            final FileTableWin fileTable = new FileTableWin(new File(rootDir, fileName));
            final MyTableIterator fileTableIterator = MyTableIterator.of(fileTable.iterator(from));
            if (fileTableIterator.hasNext()) {
                tableIterators.add(fileTableIterator);
            }
        }
        final Iterator<Row> memTableIterator = memTable.tailMap(from).values().iterator();
        if (memTableIterator.hasNext()) {
            tableIterators.add(MyTableIterator.of(memTableIterator));
        }
        final MyTableIterator[] iterators = new MyTableIterator[tableIterators.size()];
        final MergingTableIterator mergingTableIterator = new MergingTableIterator(tableIterators.toArray(iterators));
        return Iterators.transform(mergingTableIterator, row -> row.getRecord());
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws IOException {
        memTable.put(key, Row.Of(currentFileIndex, key, value, ALIVE));
        currentHeap += (Integer.BYTES
                + (key.remaining() + LINK_SIZE + Integer.BYTES * NUMBER_FIELDS_BYTEBUFFER)
                + (value.remaining() + LINK_SIZE + Integer.BYTES * NUMBER_FIELDS_BYTEBUFFER)
                + Integer.BYTES);
        checkHeap();
    }

    private void dump() throws IOException {
        final String fileTableName = PREFIX + currentFileIndex + SUFFIX;
        currentFileIndex++;
        //FileTableNotWin.write(new File(rootDir, fileTableName), memTable.values().iterator());
        FileTableWin.write(new File(rootDir, fileTableName), memTable.values().iterator());
    }

    @Override
    public void remove(@NotNull ByteBuffer key) throws IOException {
        final Row removedRow = memTable.put(key, Row.Of(currentFileIndex, key, TOMBSTONE, DEAD));
        if (removedRow == null) {
            currentHeap += (Integer.BYTES
                    + (key.remaining() + LINK_SIZE + Integer.BYTES * NUMBER_FIELDS_BYTEBUFFER)
                    + (LINK_SIZE + Integer.BYTES * NUMBER_FIELDS_BYTEBUFFER)
                    + Integer.BYTES);
        } else if (!removedRow.isDead()) {
            currentHeap -= (removedRow.getValue().remaining());
        }
        checkHeap();
    }

    private void checkHeap() throws IOException {
        if (currentHeap >= maxHeap) {
            dump();
            currentHeap = 0;
            memTable.clear();
        }
    }

    @Override
    public void close() throws IOException {
        dump();
    }
}
