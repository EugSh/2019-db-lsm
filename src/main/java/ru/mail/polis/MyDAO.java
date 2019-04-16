package ru.mail.polis;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.jetbrains.annotations.NotNull;

public class MyDAO implements DAO {
    private final NavigableMap<ByteBuffer, Record> dataBase = new TreeMap<>()

    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        return dataBase.tailMap(from).values().iterator();
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        dataBase.put(key,Record.of(key,value));
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        dataBase.remove(key);
    }

    @Override
    public void close() throws IOException {
        //nothing
    }
}
