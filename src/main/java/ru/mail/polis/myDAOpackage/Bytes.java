package ru.mail.polis.myDAOpackage;

import java.nio.ByteBuffer;

public class Bytes {
    public static ByteBuffer fromInt(int i) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(i).rewind();
    }
}
