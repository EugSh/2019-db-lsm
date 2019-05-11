package ru.mail.polis.shkalev;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.jetbrains.annotations.NotNull;

import com.google.common.collect.Iterators;

import ru.mail.polis.DAO;
import ru.mail.polis.Record;

public class MySuperDAO implements DAO {
    static final ByteBuffer TOMBSTONE = ByteBuffer.allocate(0);
    static final int ALIVE = 1;
    static final int DEAD = 0;
    private static final int MODEL = Integer.parseInt(System.getProperty("sun.arch.data.model"));
    private static final int LINK_SIZE = MODEL == 64 ? 8 : 4;
    private static final int NUMBER_FIELDS_BYTEBUFFER = 7;
    static final String PREFIX = "FT";
    static final String SUFFIX = ".mydb";
    private final NavigableMap<ByteBuffer, Row> memTable = new TreeMap<>();
    private final long maxHeap;
    private final File rootDir;
    private int currentFileIndex;
    private long currentHeap;
    private final List<FileTable> tables;

    /**
     * Creates LSM storage.
     *
     * @param maxHeap threshold of size of the memTable
     * @param rootDir the folder in which files will be written and read
     * @throws IOException if an I/O error is thrown by a File walker
     */
    public MySuperDAO(@NotNull final long maxHeap, @NotNull final File rootDir) throws IOException {
        this.maxHeap = maxHeap;
        this.rootDir = rootDir;
        this.currentHeap = 0;
        this.tables = new ArrayList<>();
        this.currentFileIndex = 0;
        final EnumSet<FileVisitOption> options = EnumSet.of(FileVisitOption.FOLLOW_LINKS);
        final int maxDeep = 1;
        Files.walkFileTree(rootDir.toPath(), options, maxDeep, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
                if (file.getFileName().toString().startsWith(PREFIX)
                        && file.getFileName().toString().endsWith(SUFFIX)) {
                    final FileTable fileTable = new FileTable(new File(rootDir, file.getFileName().toString()));
                    tables.add(fileTable);
                    currentFileIndex++;
                }
                return FileVisitResult.CONTINUE;
            }
        });
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        final List<MyTableIterator> tableIterators = new LinkedList<>();
        for (final FileTable fileT : tables) {
            tableIterators.add(MyTableIterator.of(fileT.iterator(from)));
        }

        final Iterator<Row> memTableIterator = memTable.tailMap(from).values().iterator();
        if (memTableIterator.hasNext()) {
            tableIterators.add(MyTableIterator.of(memTableIterator));
        }
        final Iterator<Row> result = MyTableIterator.getActualRowIterator(tableIterators);
        return Iterators.transform(result, row -> row.getRecord());
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        memTable.put(key, Row.of(currentFileIndex, key, value, ALIVE));
        currentHeap += Integer.BYTES
                + (long) (key.remaining() + LINK_SIZE + Integer.BYTES * NUMBER_FIELDS_BYTEBUFFER)
                + (long) (value.remaining() + LINK_SIZE + Integer.BYTES * NUMBER_FIELDS_BYTEBUFFER)
                + Integer.BYTES;
        checkHeap();
    }

    private void dump() throws IOException {
        final String fileTableName = PREFIX + currentFileIndex + SUFFIX;
        currentFileIndex++;
        final File table = new File(rootDir, fileTableName);
        FileTable.write(table, memTable.values().iterator());
        tables.add(new FileTable(table));
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        final Row removedRow = memTable.put(key, Row.of(currentFileIndex, key, TOMBSTONE, DEAD));
        if (removedRow == null) {
            currentHeap += Integer.BYTES
                    + (long) (key.remaining() + LINK_SIZE + Integer.BYTES * NUMBER_FIELDS_BYTEBUFFER)
                    + (long) (LINK_SIZE + Integer.BYTES * NUMBER_FIELDS_BYTEBUFFER)
                    + Integer.BYTES;
        } else if (!removedRow.isDead()) {
            currentHeap -= removedRow.getValue().remaining();
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
        if (currentHeap != 0) {
            dump();
        }
        for (final FileTable table : tables) {
            table.close();
        }
    }

    /**
     * Perform compaction.
     */
    @Override
    public void compact() throws IOException {
        CompactUtil.compactFile(rootDir, tables);
        currentFileIndex = tables.size();
    }
}
