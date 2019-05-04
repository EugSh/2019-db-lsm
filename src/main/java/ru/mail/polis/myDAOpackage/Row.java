package ru.mail.polis.myDAOpackage;

import java.nio.ByteBuffer;

import org.jetbrains.annotations.NotNull;

import ru.mail.polis.Record;

public class Row implements Comparable<Row> {
    private final int index;
    private final ByteBuffer key;
    private final ByteBuffer value;
    private final int status;


    private Row(@NotNull final int index,@NotNull final ByteBuffer key,@NotNull final ByteBuffer value,@NotNull final int status) {
        this.index = index;
        this.key = key;
        this.value = value;
        this.status = status;
    }

    public static Row Of(@NotNull final int index,@NotNull final ByteBuffer key,@NotNull final ByteBuffer value,@NotNull final int status) {
        return new Row(index, key, value, status);
    }

    public Record getRecord() {
        if (isDead()) {
            return Record.of(MySuperDAO.TOMBSTONE, MySuperDAO.TOMBSTONE);
        } else {
            return Record.of(key, value);
        }
    }

    public boolean isDead() {
        return status == MySuperDAO.DEAD;
    }

    public ByteBuffer getKey() {
        return key.asReadOnlyBuffer();
    }

    public ByteBuffer getValue() {
        return value.asReadOnlyBuffer();
    }

    public int getIndex() {
        return index;
    }

    @Override
    public int compareTo(@NotNull final Row o) {
        if (isDead()){
            return -1;
        }
        if (o.isDead()){
            return 1;
        }
        if (key.compareTo(o.getKey()) == 0) {
            return -Integer.compare(index, o.getIndex());
        }
        return key.compareTo(o.getKey());
    }
}
