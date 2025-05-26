package edu.mora.db.storage;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashSet;
import java.util.Set;

public class WALManager {

    private static final String LOG_FILE = "wal.log";
    private final RandomAccessFile logFile;

    public WALManager(String dbPath) throws IOException {
        logFile = new RandomAccessFile(dbPath + "/" + LOG_FILE, "rw");
        logFile.seek(logFile.length());            // append mode
    }

    /* --------------- public log helpers ---------------- */
    public synchronized void logBegin(long txId) throws IOException {
        writeHdr((byte) 1, txId);
    }

    public synchronized void logCommit(long txId) throws IOException {
        writeHdr((byte) 3, txId);
    }

    public synchronized void logAbort(long txId) throws IOException {
        writeHdr((byte) 4, txId);
    }

    public synchronized void logUpdate(long txId, int pageId,
                                       byte[] before, byte[] after) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeInt(pageId);

        dos.writeInt(before.length);
        dos.write(before);
        dos.writeInt(after.length);
        dos.write(after);

        byte[] payload = baos.toByteArray();
        writeHdr((byte) 2, txId, payload);
    }

    public synchronized void flush() throws IOException {
        logFile.getFD().sync();
    }

    /* --------------- recovery (same as before) ---------- */
    public void recover(BufferPool pool, DiskManager disk) throws IOException {
        logFile.seek(0);
        Set<Long> begun = new HashSet<>(), committed = new HashSet<>();

        while (logFile.getFilePointer() < logFile.length()) {
            int len = logFile.readInt();
            byte typ = logFile.readByte();
            long tx = logFile.readLong();
            switch (typ) {
                case 1 -> begun.add(tx);
                case 3 -> committed.add(tx);
            }
            logFile.skipBytes(len - 1 - 8);
        }

        logFile.seek(0);
        while (logFile.getFilePointer() < logFile.length()) {
            int len = logFile.readInt();
            byte typ = logFile.readByte();
            long tx = logFile.readLong();

            if (typ == 2 && committed.contains(tx)) {
                int pageId = logFile.readInt();
                int bLen = logFile.readInt();
                byte[] before = new byte[bLen];
                logFile.readFully(before);
                int aLen = logFile.readInt();
                byte[] after = new byte[aLen];
                logFile.readFully(after);
                disk.writePage(pageId, after);                  // REDO
            } else {
                logFile.skipBytes(len - 1 - 8);
            }
        }
    }

    public void close() throws IOException {
        logFile.close();
    }

    /* --------------- private  --------------------------- */
    private void writeHdr(byte type, long txId) throws IOException {
        writeHdr(type, txId, new byte[0]);
    }

    private void writeHdr(byte type, long txId, byte[] payload) throws IOException {
        int len = 1 + 8 + payload.length;
        logFile.writeInt(len);
        logFile.writeByte(type);
        logFile.writeLong(txId);
        logFile.write(payload);
    }
}