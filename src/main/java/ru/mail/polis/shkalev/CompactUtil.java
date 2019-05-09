package ru.mail.polis.shkalev;

import java.io.File;
import java.io.IOException;
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

import com.google.common.collect.Iterators;

import ru.mail.polis.Iters;

public final class CompactUtil {
    private static final String TMP = ".tmp";
    private static final int startFileIndex = 0;

    private CompactUtil() {
    }

    public static FileTable compactFileTables(File rootDir, Collection<FileTable> fileTables) throws IOException {
        final List<MyTableIterator> tableIterators = new LinkedList<>();
        for (final FileTable fileT : fileTables) {
            tableIterators.add(MyTableIterator.of(fileT.iterator()));
        }
        final Iterator<Row> mergingTableIterator = Iterators.mergeSorted(tableIterators, Row::compareTo);
        final Iterator<Row> collapsedIterator = Iters.collapseEquals(mergingTableIterator, Row::getKey);
        final Iterator<Row> filteredRow = Iterators.filter(collapsedIterator, row -> !row.isDead());
        final File compactFileTmp = compact(rootDir, filteredRow);
        closeTables(fileTables);
        clearDirectory(rootDir);
        final String fileDbName = MySuperDAO.PREFIX + startFileIndex + MySuperDAO.SUFFIX;
        final File compactFileDb = new File(rootDir, fileDbName);
        Files.move(compactFileTmp.toPath(), compactFileDb.toPath(), StandardCopyOption.ATOMIC_MOVE);
        return new FileTable(compactFileDb);
    }

    private static File compact(File rootDir, Iterator<Row> rows) throws IOException {
        final String fileTableName = MySuperDAO.PREFIX + startFileIndex + TMP;
        final File table = new File(rootDir, fileTableName);
        FileTable.write(table, rows);
        return table;
    }

    private static void closeTables(Collection<FileTable> tables) throws IOException {
        for (final FileTable table : tables) {
            table.close();
        }
    }

    private static void clearDirectory(File dir) throws IOException {
        final EnumSet<FileVisitOption> options = EnumSet.of(FileVisitOption.FOLLOW_LINKS);
        final int maxDeep = 1;
        Files.walkFileTree(dir.toPath(), options, maxDeep, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs){
                if (file.getFileName().toString().startsWith(MySuperDAO.PREFIX)
                        && file.getFileName().toString().endsWith(MySuperDAO.SUFFIX)) {
                    file.toFile().delete();
                }
                return FileVisitResult.CONTINUE;
            }
        });
    }
}
