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
        this.logFile = new RandomAccessFile(dbPath + "/" + LOG_FILE, "rw");
        // Position at end for appends
        logFile.seek(logFile.length());
    }

    public synchronized void logBegin(long txId) throws IOException {
        writeRecord((byte) 1, txId, null, null);
    }

    public synchronized void logUpdate(long txId, int pageId, byte[] before, byte[] after) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeInt(pageId);
        // write before-image length + data
        dos.writeInt(before.length);
        dos.write(before);
        // write after-image length + data
        dos.writeInt(after.length);
        dos.write(after);
        writeRecord((byte) 2, txId, baos.toByteArray(), null);
    }

    public synchronized void logCommit(long txId) throws IOException {
        writeRecord((byte) 3, txId, null, null);
        logFile.getFD().sync(); // durable commit
    }

    public synchronized void logAbort(long txId) throws IOException {
        writeRecord((byte) 4, txId, null, null);
        logFile.getFD().sync();
    }

    private void writeRecord(byte type, long txId, byte[] payload, Object unused) throws IOException {
        payload = (payload == null ? new byte[0] : payload);
        int len = 1 + 8 + payload.length; // type + txId + body
        logFile.writeInt(len);
        logFile.writeByte(type);
        logFile.writeLong(txId);
        logFile.write(payload);
    }

    /**
     * On startup, call this to replay the log: 1) Scan forward, record committed TXs 2) ROLLBACK uncommitted ones (undo
     * their updates) 3) REDO committed updates
     */
    public void recover(BufferPool bufferPool, DiskManager diskManager) throws IOException {
        logFile.seek(0);
        Set<Long> committed = new HashSet<>();
        Set<Long> started = new HashSet<>();

        // First pass: identify started and committed TXs
        while (logFile.getFilePointer() < logFile.length()) {
            int recLen = logFile.readInt();
            byte type = logFile.readByte();
            long txId = logFile.readLong();
            if (type == 1) started.add(txId);
            if (type == 3) committed.add(txId);

            // skip rest of record
            logFile.skipBytes(recLen - 1 - 8);
        }

        // Second pass: redo all committed updates
        logFile.seek(0);
        while (logFile.getFilePointer() < logFile.length()) {
            int recLen = logFile.readInt();
            byte type = logFile.readByte();
            long txId = logFile.readLong();

            if (type == 2 && committed.contains(txId)) {
                // payload: pageId + before + after
                int pageId = logFile.readInt();
                int beforeLen = logFile.readInt();
                byte[] before = new byte[beforeLen];
                logFile.readFully(before);
                int afterLen = logFile.readInt();
                byte[] after = new byte[afterLen];
                logFile.readFully(after);

                // apply redo
                diskManager.writePage(pageId, after);
            } else {
                // skip payload
                logFile.skipBytes(recLen - 1 - 8);
            }
        }
    }

    public void close() throws IOException {
        logFile.close();
    }
}