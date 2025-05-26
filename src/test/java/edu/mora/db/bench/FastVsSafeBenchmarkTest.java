package edu.mora.db.bench;

import edu.mora.db.catalog.Catalog;
import edu.mora.db.executor.SimpleExecutor;
import edu.mora.db.storage.BufferPool;
import edu.mora.db.storage.DiskManager;
import edu.mora.db.storage.TransactionManager;
import edu.mora.db.storage.WALManager;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Micro-benchmark (definitely **not** JMH) that contrasts FAST vs SAFE commit latency/throughput under heavy write
 * contention.
 * <p>
 * Run with <pre>mvn -Dtest=FastVsSafeBenchmarkTest test</pre>
 */
public class FastVsSafeBenchmarkTest {

    @TempDir
    Path dir;

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
        // true → FAST, false → SAFE
    void runBenchmark(boolean fast) throws Exception {

        /* ───── silence SimpleExecutor chatter ───────────────────── */
        PrintStream realOut = System.out;
        System.setOut(new PrintStream(OutputStream.nullOutputStream()));

        /* ───── bootstrap a fresh in-process engine ──────────────── */
        DiskManager disk = new DiskManager(dir.toString());
        BufferPool pool = new BufferPool(64, disk);
        WALManager wal = new WALManager(dir.toString());
        TransactionManager tm = new TransactionManager(wal, pool, disk);
        Catalog cat = new Catalog(dir.toString(), pool);
        SimpleExecutor exec = new SimpleExecutor(cat, tm);

        exec.execute("CREATE TABLE kv (k INT, v INT)");
        for (int k = 0; k < 256; k++)
            exec.execute("INSERT INTO kv VALUES (" + k + ", 0)");

        /* ───── workload parameters ──────────────────────────────── */
        final int THREADS = 8;
        final long RUNTIME_SEC = 5;         // <-- change freely

        ExecutorService es = Executors.newFixedThreadPool(THREADS);
        CountDownLatch start = new CountDownLatch(1);
        AtomicLong ops = new AtomicLong();
        AtomicLong nanos = new AtomicLong();

        for (int t = 0; t < THREADS; t++) {
            es.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException ignored) {
                }

                ThreadLocalRandom rnd = ThreadLocalRandom.current();
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(RUNTIME_SEC);

                while (System.nanoTime() < deadline) {
                    int k = rnd.nextInt(256);
                    String sql = (fast ? "/*+ FAST */ " : "/*+ SAFE */ ")
                            + "UPDATE kv SET v = v + 1 WHERE k = " + k;
                    long beg = System.nanoTime();
                    try {
                        exec.execute(sql);
                        long end = System.nanoTime();
                        ops.incrementAndGet();
                        nanos.addAndGet(end - beg);
                    } catch (Exception e) {
                        System.out.println("Failed to execute: " + sql + e.getMessage());
                        /* All failures here are due to benign write-write
                           races (e.g. row was just relocated).  Skip them and
                           keep hammering so the loop can run for the full
                           duration. */
                    }
                }
            });
        }

        /* ───── go! ──────────────────────────────────────────────── */
        start.countDown();
        es.shutdown();
        es.awaitTermination(RUNTIME_SEC + 10, TimeUnit.SECONDS);

        /* ───── restore console & print summary ──────────────────── */
        System.setOut(realOut);

        double avgµs = ops.get() == 0 ? Double.NaN
                : nanos.get() / (double) ops.get() / 1_000;
        double tpS = ops.get() / (double) RUNTIME_SEC;

        System.out.printf("%s commit → %,d tx  |  throughput %.1f tx/s  |  avg %.1f µs%n",
                          fast ? "FAST" : "SAFE", ops.get(), tpS, avgµs);

        /* ───── tidy up ──────────────────────────────────────────── */
        tm.close();
        disk.close();
    }
}