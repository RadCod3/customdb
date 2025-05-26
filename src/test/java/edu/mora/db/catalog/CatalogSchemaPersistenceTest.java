package edu.mora.db.catalog;

import edu.mora.db.storage.BufferPool;
import edu.mora.db.storage.DiskManager;
import edu.mora.db.table.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Creates two tables, restarts the Catalog, and checks that every schema detail has been reloaded correctly.
 */
class CatalogSchemaPersistenceTest {

    @TempDir
    Path dir;

    @Test
    void schemasSurviveRestart() throws IOException {
        /* ---------- boot #1: create tables ---------- */
        DiskManager d1 = new DiskManager(dir.toString());
        BufferPool p1 = new BufferPool(16, d1);
        Catalog c1 = new Catalog(dir.toString(), p1);

        Schema people = new Schema(
                java.util.List.of("id", "name"),
                java.util.List.of(Schema.Type.INT, Schema.Type.STRING));

        Schema pets = new Schema(
                java.util.List.of("tag", "species"),
                java.util.List.of(Schema.Type.INT, Schema.Type.STRING));

        c1.createTable("people", people);
        c1.createTable("pets", pets);
        p1.flushAll();    // make sure any dirty pages are on disk
        d1.close();

        /* ---------- boot #2: reload catalog ---------- */
        DiskManager d2 = new DiskManager(dir.toString());
        BufferPool p2 = new BufferPool(16, d2);
        Catalog c2 = new Catalog(dir.toString(), p2);

        Set<String> names = c2.listTables();
        assertEquals(Set.of("people", "pets"), names);

        Schema reloaded = c2.getTable("people").getSchema();
        assertEquals(2, reloaded.numColumns());
        assertEquals("name", reloaded.getColumnName(1));
        assertEquals(Schema.Type.STRING, reloaded.getColumnType(1));

        d2.close();
    }
}