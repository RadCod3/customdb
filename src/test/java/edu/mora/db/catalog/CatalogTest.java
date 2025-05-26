//package edu.mora.db.catalog;
//
//import edu.mora.db.storage.DiskManager;
//import edu.mora.db.storage.BufferPool;
//import edu.mora.db.table.Schema;
//import edu.mora.db.table.Tuple;
//import edu.mora.db.table.Table;
//import edu.mora.db.storage.RecordId;
//import org.junit.jupiter.api.*;
//
//import java.io.IOException;
//import java.nio.file.Files;
//import java.nio.file.Path;
//import java.util.Arrays;
//import java.util.List;
//import java.util.Set;
//
//import static org.junit.jupiter.api.Assertions.*;
//
//class CatalogTest {
//    private Path tempDir;
//    private DiskManager disk;
//    private BufferPool buf;
//    private Catalog catalog;
//    private Schema schema;
//
//    @BeforeEach
//    void setup() throws IOException {
//        tempDir = Files.createTempDirectory("catalogtest");
//        disk = new DiskManager(tempDir.toString());
//        buf  = new BufferPool(10, disk);
//        catalog = new Catalog(tempDir.toString(), buf);
//
//        // common schema
//        schema = new Schema(
//                Arrays.asList("id", "name"),
//                Arrays.asList(Schema.Type.INT, Schema.Type.STRING)
//        );
//    }
//
//    @AfterEach
//    void teardown() throws IOException {
//        disk.close();
//        // Note: tempDir deletion is optional; OS will clean up
//    }
//
//    @Test
//    void testCreateAndListMultipleTables() throws IOException {
//        catalog.createTable("users", schema);
//        catalog.createTable("orders", schema);
//
//        Set<String> tables = catalog.listTables();
//        assertEquals(2, tables.size());
//        assertTrue(tables.contains("users"));
//        assertTrue(tables.contains("orders"));
//    }
//
//    @Test
//    void testIsolationBetweenTables() throws IOException {
//        catalog.createTable("a", schema);
//        catalog.createTable("b", schema);
//
//        Table ta = catalog.getTable("a");
//        Table tb = catalog.getTable("b");
//
//        // Insert into 'a'
//        RecordId r1 = ta.insertTuple(new Tuple(schema, 1, "Alice"));
//        // Insert into 'b'
//        RecordId r2 = tb.insertTuple(new Tuple(schema, 2, "Bob"));
//
//        List<Tuple> la = ta.scanAll();
//        List<Tuple> lb = tb.scanAll();
//
//        assertEquals(1, la.size());
//        assertEquals(1, lb.size());
//        assertEquals("Alice", la.get(0).getField(1));
//        assertEquals("Bob",   lb.get(0).getField(1));
//    }
//
//    @Test
//    void testCatalogPersistence() throws IOException {
//        catalog.createTable("persist", schema);
//        // close and reopen catalog
//        disk.close();
//        Catalog loaded = new Catalog(tempDir.toString(), buf);
//
//        List<String> tables = List.copyOf(loaded.listTables());
//        assertTrue(tables.contains("persist"));
//
//        // table instance should allow operations
//        Table t = loaded.getTable("persist");
//        RecordId rid = t.insertTuple(new Tuple(schema, 5, "Five"));
//        assertEquals(1, t.scanAll().size());
//        assertEquals("Five", t.readTuple(rid).getField(1));
//    }
//}