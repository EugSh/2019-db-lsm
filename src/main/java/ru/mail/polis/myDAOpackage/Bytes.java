package ru.mail.polis.myDAOpackage;

import java.nio.ByteBuffer;

import com.google.common.primitives.Ints;

public class Bytes {
    public static ByteBuffer fromInt(int i){
        return ByteBuffer.allocate(Integer.BYTES).putInt(i).rewind();
    }
}
