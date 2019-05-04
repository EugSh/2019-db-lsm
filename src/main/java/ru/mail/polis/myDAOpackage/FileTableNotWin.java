package ru.mail.polis.myDAOpackage;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.jetbrains.annotations.NotNull;

public class FileTableNotWin {
    final ByteBuffer rows;
    final IntBuffer offsets;
    final int count;
    final int fileIndex;
    final ByteBuffer mmap;

    public FileTableNotWin(@NotNull final File file) throws IOException {
        fileIndex = Integer.parseInt(file.getName().substring(2, file.getName().length() - 4));
        try (FileChannel fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
            final ByteBuffer map = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size()).order(ByteOrder.BIG_ENDIAN);
            mmap = map;
            assert fileChannel.size() < Integer.MAX_VALUE;
            final int size = (int) fileChannel.size();
            final ByteBuffer countBB = map.duplicate()
                    .position(size - Integer.BYTES);
            count = countBB.getInt();
            final ByteBuffer offsetsBB = map.duplicate().position(size - Integer.BYTES * count - Integer.BYTES)
                    .limit(size - Integer.BYTES);
            offsets = offsetsBB.slice().asIntBuffer();
            rows = map.duplicate()
                    .limit(size - Integer.BYTES * count - Integer.BYTES)
                    .slice();
        }
    }

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
                return getRowAt(index++);
            }
        };
    }

    private int getOffsetsIndex(@NotNull final ByteBuffer from) {
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

    private ByteBuffer getKeyAt(@NotNull final int i) {
        assert 0 <= i && i < count;
        final int offset = offsets.get(i);
        final int keySize = rows.getInt(offset);
        final ByteBuffer keyBB = rows.duplicate()
                .position(offset + Integer.BYTES)
                .limit(offset + Integer.BYTES + keySize);
        return keyBB.slice();
    }

    private Row getRowAt(@NotNull final int i) {
        assert 0 <= i && i < count;
        int offset = offsets.get(i);

        //Key
        final int keySize = rows.getInt(offset);
        offset += Integer.BYTES;
        final ByteBuffer keyBB = rows.duplicate()
                .position(offset)
                .limit(offset + keySize);
        offset += keySize;

        //Value
        final int valueSize = rows.getInt(offset);
        offset += Integer.BYTES;
        final int status = rows.getInt(offset);
        offset += Integer.BYTES;
        if (status == MySuperDAO.DEAD) {
            return Row.Of(fileIndex, keyBB.slice(), MySuperDAO.TOMBSTONE, status);
        } else {
            final ByteBuffer valueBB = rows.duplicate()
                    .position(offset)
                    .limit(offset + valueSize);
            return Row.Of(fileIndex, keyBB.slice(), valueBB.slice(), status);
        }
    }

    public static void write(@NotNull final File to, @NotNull final Iterator<Row> rows) throws IOException {
        try (FileChannel fileChannel = FileChannel.open(to.toPath(), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            final List<Integer> offsets = new ArrayList<>();
            int offset = 0;
            while (rows.hasNext()) {
                offsets.add(offset);
                final Row row = rows.next();

                //Key
                fileChannel.write(Bytes.fromInt(row.getKey().remaining()));
                fileChannel.write(row.getKey());
                offset += (Integer.BYTES + row.getKey().remaining());

                //Value
                fileChannel.write(Bytes.fromInt(row.getValue().remaining()));
                offset += Integer.BYTES;
                if (!row.isDead()) {
                    fileChannel.write(Bytes.fromInt(MySuperDAO.ALIVE));
                    offset += Integer.BYTES;
                    fileChannel.write(row.getValue());
                    offset += row.getValue().remaining();
                } else {
                    fileChannel.write(Bytes.fromInt(MySuperDAO.DEAD));
                    offset += Integer.BYTES;
                }
            }
            for (Integer elemOffSets : offsets) {
                fileChannel.write(Bytes.fromInt(elemOffSets));
            }
            fileChannel.write(Bytes.fromInt(offsets.size()));
        }
    }
}
