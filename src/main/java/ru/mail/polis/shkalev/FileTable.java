package ru.mail.polis.shkalev;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.jetbrains.annotations.NotNull;


public class FileTable {
    final int count;
    final int fileIndex;
    final File file;

    public FileTable(@NotNull final File file) throws IOException {
        this.file = file;
        fileIndex = Integer.parseInt(file.getName().substring(2, file.getName().length() - 4));
        try (FileChannel fc = openRead(file)) {
            final ByteBuffer countBB = ByteBuffer.allocate(Integer.BYTES);
            fc.read(countBB, fc.size() - Integer.BYTES);
            countBB.rewind();
            count = countBB.getInt();
        }
    }

    private FileChannel openRead(@NotNull final File file) throws IOException {
        return FileChannel.open(file.toPath(), StandardOpenOption.READ);
    }

    /***
     * get iterator
     * @param from
     * @return
     * @throws IOException
     */
    @NotNull
    public Iterator<Row> iterator(@NotNull final ByteBuffer from) throws IOException {
        return new Iterator<Row>() {
            int index = getOffsetsIndex(from);

            @Override
            public boolean hasNext() {
                return index < count;
            }

            @Override
            public Row next() {
                assert hasNext();
                Row row = null;
                try {
                    row = getRowAt(index++);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return row;
            }
        };
    }

    private int getOffsetsIndex(@NotNull final ByteBuffer from) throws IOException {
        int left = 0;
        int right = count - 1;
        while (left <= right) {
            final int middle = left + (right - left) / 2;
            final int resCmp = from.compareTo(getKeyAt(middle));
            if (resCmp < 0) {
                right = middle - 1;
            } else if (resCmp > 0) {
                left = middle + 1;
            } else {
                return middle;
            }
        }
        return left;
    }

    private int getOffset(@NotNull final FileChannel fc,@NotNull final int i) throws IOException {
        final ByteBuffer offsetBB = ByteBuffer.allocate(Integer.BYTES);
        fc.read(offsetBB, fc.size() - Integer.BYTES - Integer.BYTES * count + Integer.BYTES * i);
        offsetBB.rewind();
        return offsetBB.getInt();
    }

    private ByteBuffer getKeyAt(@NotNull final int i) throws IOException {
        assert 0 <= i && i < count;
        try (FileChannel fc = openRead(file)) {
            final int offset = getOffset(fc, i);
            final ByteBuffer keySizeBB = ByteBuffer.allocate(Integer.BYTES);
            fc.read(keySizeBB, offset);
            keySizeBB.rewind();
            final int keySize = keySizeBB.getInt();
            final ByteBuffer keyBB = ByteBuffer.allocate(keySize);
            fc.read(keyBB, offset + Integer.BYTES);
            keyBB.rewind();
            return keyBB.slice();
        }
    }

    private Row getRowAt(@NotNull final int i) throws IOException {
        assert 0 <= i && i < count;
        try (FileChannel fc = openRead(file)) {
            int offset = getOffset(fc, i);

            //Key
            final ByteBuffer keySizeBB = ByteBuffer.allocate(Integer.BYTES);
            fc.read(keySizeBB, offset);
            keySizeBB.rewind();
            final int keySize = keySizeBB.getInt();
            offset += Integer.BYTES;
            final ByteBuffer keyBB = ByteBuffer.allocate(keySize);
            fc.read(keyBB, offset);
            keyBB.rewind();
            offset += keySize;

            //Value
            final ByteBuffer valueSizeBB = ByteBuffer.allocate(Integer.BYTES);
            fc.read(valueSizeBB, offset);
            valueSizeBB.rewind();
            final int valueSize = valueSizeBB.getInt();
            offset += Integer.BYTES;
            final ByteBuffer statusBB = ByteBuffer.allocate(Integer.BYTES);
            fc.read(statusBB, offset);
            statusBB.rewind();
            final int status = statusBB.getInt();
            statusBB.clear();
            offset += Integer.BYTES;
            if (status == MySuperDAO.DEAD) {
                return Row.of(fileIndex, keyBB.slice(), MySuperDAO.TOMBSTONE, status);
            } else {
                final ByteBuffer valueBB = ByteBuffer.allocate(valueSize);
                fc.read(valueBB, offset);
                valueBB.rewind();
                return Row.of(fileIndex, keyBB, valueBB, status);
            }
        }
    }

    /***
     * write on disk
     * @param to
     * @param rows
     * @throws IOException
     */
    public static void write(@NotNull final File to,
            @NotNull final Iterator<Row> rows) throws IOException {
        try (FileChannel fileChannel = FileChannel.open(to.toPath(),
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE)) {
            final List<Integer> offsets = new ArrayList<>();
            int offset = 0;
            while (rows.hasNext()) {
                offsets.add(offset);
                final Row row = rows.next();

                //Key
                fileChannel.write(Bytes.fromInt(row.getKey().remaining()));
                fileChannel.write(row.getKey());
                offset += Integer.BYTES + row.getKey().remaining();

                //Value
                fileChannel.write(Bytes.fromInt(row.getValue().remaining()));
                offset += Integer.BYTES;
                if (row.isDead()) {
                    fileChannel.write(Bytes.fromInt(MySuperDAO.DEAD));
                    offset += Integer.BYTES;
                } else {
                    fileChannel.write(Bytes.fromInt(MySuperDAO.ALIVE));
                    offset += Integer.BYTES;
                    fileChannel.write(row.getValue());
                    offset += row.getValue().remaining();
                }
            }
            for (final Integer elemOffSets : offsets) {
                fileChannel.write(Bytes.fromInt(elemOffSets));
            }
            fileChannel.write(Bytes.fromInt(offsets.size()));
        }
    }
}
