//package edu.mora.db.table;
//
//import edu.mora.db.storage.BufferPool;
//import edu.mora.db.storage.DiskManager;
//import edu.mora.db.storage.RecordId;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//
//import java.io.IOException;
//import java.util.Arrays;
//
//import static org.junit.jupiter.api.Assertions.*;
//
//class TableTest {
//    private DiskManager disk;
//    private BufferPool buf;
//    private Table table;
//    private Schema schema;
//
//    @BeforeEach
//    void setup() throws IOException {
//        // use a temp directory or in-memory path
//        disk = new DiskManager("src/test/resources");
//        buf = new BufferPool(5, disk);
//        schema = new Schema(
//                Arrays.asList("id", "name"),
//                Arrays.asList(Schema.Type.INT, Schema.Type.STRING)
//        );
//        table = new Table(schema, buf, disk);
//    }
//
//    @Test
//    void insertAndRead() throws IOException {
//        Tuple t1 = new Tuple(schema, 42, "Alice");
//        RecordId rid = table.insertTuple(t1);
//
//        Tuple t2 = table.readTuple(rid);
//        assertEquals(42, t2.getField(0));
//        assertEquals("Alice", t2.getField(1));
//    }
//
//    @Test
//    void deleteTuple() throws IOException {
//        Tuple t1 = new Tuple(schema, 7, "Bob");
//        RecordId rid = table.insertTuple(t1);
//
//        table.deleteTuple(rid);
//        // reading a tombstoned record should fail
//        assertThrows(IllegalStateException.class, () -> table.readTuple(rid));
//    }
//
//    @Test
//    void updateInPlace() throws IOException {
//        Tuple t1 = new Tuple(schema, 1, "Ann");
//        RecordId rid = table.insertTuple(t1);
//
//        // smaller name fits in place
//        Tuple t2 = new Tuple(schema, 2, "Al");
//        RecordId rid2 = table.updateTuple(rid, t2);
//        assertEquals(rid, rid2);
//
//        Tuple got = table.readTuple(rid);
//        assertEquals(2, got.getField(0));
//        assertEquals("Al", got.getField(1));
//    }
//
//    @Test
//    void updateRelocate() throws IOException {
//        // initial short name
//        Tuple t1 = new Tuple(schema, 3, "Ed");
//        RecordId rid1 = table.insertTuple(t1);
//
//        // longer name forces relocation
//        Tuple t2 = new Tuple(schema, 4, "Edward");
//        RecordId rid2 = table.updateTuple(rid1, t2);
//
//        assertNotEquals(rid1, rid2);
//
//        // new RID reads correctly
//        Tuple gotNew = table.readTuple(rid2);
//        assertEquals(4, gotNew.getField(0));
//        assertEquals("Edward", gotNew.getField(1));
//
//        // old slot is tombstoned
//        assertThrows(IllegalStateException.class, () -> table.readTuple(rid1));
//    }
//}