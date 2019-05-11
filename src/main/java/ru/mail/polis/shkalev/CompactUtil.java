package ru.mail.polis.shkalev;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.jetbrains.annotations.NotNull;

public final class CompactUtil {
    private static final String TMP = ".tmp";
    private static final int START_FILE_INDEX = 0;

    private CompactUtil() {
    }

    /**
     * Compact files.
     *
     * @param rootDir    base directory
     * @param fileTables list file tables
     * @throws IOException if an I/O error is thrown by FileTable.iterator
     */
    public static void compactFile(@NotNull final File rootDir,
            @NotNull final Collection<FileTable> fileTables) throws IOException {
        final List<MyTableIterator> tableIterators = new LinkedList<>();
        for (final FileTable fileT : fileTables) {
            tableIterators.add(MyTableIterator.of(fileT.iterator(ByteBuffer.allocate(0))));
        }
        final Iterator<Row> filteredRow = MyTableIterator.getActualRowIterator(tableIterators);
        final File compactFileTmp = compact(rootDir, filteredRow);
        for (final FileTable fileTable :
                fileTables) {
            fileTable.close();
            fileTable.deleteFile();
        }
        fileTables.clear();
        final String fileDbName = MySuperDAO.PREFIX + START_FILE_INDEX + MySuperDAO.SUFFIX;
        final File compactFileDb = new File(rootDir, fileDbName);
        Files.move(compactFileTmp.toPath(), compactFileDb.toPath(), StandardCopyOption.ATOMIC_MOVE);
        fileTables.add(new FileTable(compactFileDb));
    }

    private static File compact(@NotNull final File rootDir,
            @NotNull final Iterator<Row> rows) throws IOException {
        final String fileTableName = MySuperDAO.PREFIX + START_FILE_INDEX + TMP;
        final File table = new File(rootDir, fileTableName);
        FileTable.write(table, rows);
        return table;
    }
}
