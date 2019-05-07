package ru.mail.polis.shkalev;

import java.nio.ByteBuffer;

import org.jetbrains.annotations.NotNull;

public final class Bytes {
    private Bytes() {
        // Not instantiatable
    }

    public static ByteBuffer fromInt(@NotNull final int i) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(i).rewind();
    }
}
