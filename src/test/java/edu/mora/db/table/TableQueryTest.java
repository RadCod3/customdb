//package edu.mora.db.table;
//
//import edu.mora.db.storage.BufferPool;
//import edu.mora.db.storage.DiskManager;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//
//import java.io.IOException;
//import java.util.Arrays;
//import java.util.List;
//
//import static org.junit.jupiter.api.Assertions.assertEquals;
//import static org.junit.jupiter.api.Assertions.assertTrue;
//
//class TableQueryTest {
//    private Table table;
//
//    @BeforeEach
//    void setup() throws IOException {
//        DiskManager disk = new DiskManager("src/test/resources");
//        BufferPool buf = new BufferPool(5, disk);
//        Schema schema = new Schema(
//                Arrays.asList("id", "name"),
//                Arrays.asList(Schema.Type.INT, Schema.Type.STRING)
//        );
//        table = new Table(schema, buf, disk);
//
//        // populate
//        table.insertTuple(new Tuple(schema, 1, "Alice"));
//        table.insertTuple(new Tuple(schema, 2, "Bob"));
//        table.insertTuple(new Tuple(schema, 3, "Charlie"));
//    }
//
//    @Test
//    void scanAllReturnsAll() throws IOException {
//        List<Tuple> all = table.scanAll();
//        assertEquals(3, all.size());
//    }
//
//    @Test
//    void scanWithPredicateFilters() throws IOException {
//        List<Tuple> result = table.scan(t -> (int) t.getField(0) % 2 == 1);
//        assertEquals(2, result.size());
//        assertTrue(result.stream().anyMatch(t -> t.getField(1).equals("Alice")));
//        assertTrue(result.stream().anyMatch(t -> t.getField(1).equals("Charlie")));
//    }
//}