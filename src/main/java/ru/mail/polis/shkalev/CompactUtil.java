package ru.mail.polis.shkalev;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.jetbrains.annotations.NotNull;

import com.google.common.collect.Iterators;

import ru.mail.polis.Iters;

public final class CompactUtil {
    private static final String TMP = ".tmp";
    private static final int START_FILE_INDEX = 0;

    private CompactUtil() {
    }

    /**
     * Compact files.
     * @param rootDir base directory
     * @param fileTables list file tables
     * @return file table which consist actual data
     * @throws IOException if an I/O error is thrown by FileTable.iterator
     */
    public static FileTable compactFileTables(@NotNull final File rootDir,
            @NotNull final Collection<FileTable> fileTables) throws IOException {
        final List<MyTableIterator> tableIterators = new LinkedList<>();
        for (final FileTable fileT : fileTables) {
            tableIterators.add(MyTableIterator.of(fileT.iterator(ByteBuffer.allocate(0))));
        }
        final Iterator<Row> mergingTableIterator = Iterators.mergeSorted(tableIterators, Row::compareTo);
        final Iterator<Row> collapsedIterator = Iters.collapseEquals(mergingTableIterator, Row::getKey);
        final Iterator<Row> filteredRow = Iterators.filter(collapsedIterator, row -> !row.isDead());
        final File compactFileTmp = compact(rootDir, filteredRow);
        closeTables(fileTables);
        clearDirectory(rootDir);
        final String fileDbName = MySuperDAO.PREFIX + START_FILE_INDEX + MySuperDAO.SUFFIX;
        final File compactFileDb = new File(rootDir, fileDbName);
        Files.move(compactFileTmp.toPath(), compactFileDb.toPath(), StandardCopyOption.ATOMIC_MOVE);
        return new FileTable(compactFileDb);
    }

    private static File compact(@NotNull final File rootDir,
            @NotNull final Iterator<Row> rows) throws IOException {
        final String fileTableName = MySuperDAO.PREFIX + START_FILE_INDEX + TMP;
        final File table = new File(rootDir, fileTableName);
        FileTable.write(table, rows);
        return table;
    }

    private static void closeTables(@NotNull final Collection<FileTable> tables) throws IOException {
        for (final FileTable table : tables) {
            table.close();
        }
    }

    private static void clearDirectory(@NotNull final File dir) throws IOException {
        final EnumSet<FileVisitOption> options = EnumSet.of(FileVisitOption.FOLLOW_LINKS);
        final int maxDeep = 1;
        Files.walkFileTree(dir.toPath(), options, maxDeep, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
                if (file.getFileName().toString().startsWith(MySuperDAO.PREFIX)
                        && file.getFileName().toString().endsWith(MySuperDAO.SUFFIX)) {
                    Files.delete(file);
                }
                return FileVisitResult.CONTINUE;
            }
        });
    }
}
