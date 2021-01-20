package crux.api;

import clojure.lang.Keyword;

import java.time.Instant;
import java.util.*;
import java.util.function.Consumer;

import crux.api.tx.*;

import org.junit.*;

import static crux.api.TestUtils.*;
import static org.junit.Assert.*;

public class TransactionTest {
    private static class PersonDocument extends AbstractCruxDocument {
        private final String id;
        private final String name;
        private final String lastName;
        private final int version;

        private PersonDocument(String id, String name, String lastName, int version) {
            this.id = id;
            this.name = name;
            this.lastName = lastName;
            this.version = version;
        }

        @Override
        public Object getId() {
            return id;
        }

        @Override
        public Map<Keyword, Object> getData() {
            HashMap<Keyword, Object> ret = new HashMap<>();
            ret.put(Keyword.intern("person/name"), name);
            ret.put(Keyword.intern("person/lastName"), lastName);
            ret.put(Keyword.intern("person/version"), version);
            return ret;
        }
    }

    private static final String pabloId = "PabloPicasso";
    private static List<Date> times;
    private static List<PersonDocument> pablos;
    private static ICruxAPI node = null;

    @BeforeClass
    public static void beforeClass() {
        ArrayList<Date> _times = new ArrayList<>();
        ArrayList<PersonDocument> _pablos = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            //The few hours after Y2K
            long seconds = 946684800 + i * 3600;
            Date time = Date.from(Instant.ofEpochSecond(seconds));
            _times.add(time);

            PersonDocument pablo = new PersonDocument(pabloId, "Pablo", "Picasso", i);
            _pablos.add(pablo);
        }
        times = _times;
        pablos = _pablos;
    }

    @Before
    public void before() {
        node = Crux.startNode();
    }

    @After
    public void after() {
        close(node);
        node = null;
    }

    @AfterClass
    public static void afterClass() {
        times = null;
        pablos = null;
    }

    @Test
    public void putNow() {
        submitTx(false, tx -> {
            tx.put(pablos.get(0));
        });

        assertPabloVersion(0);
    }

    @Test
    public void putAtTime() {
        submitTx(false, tx -> {
            tx.put(pablo(0), time(1));
        });

        assertNoPablo(0);
        assertPabloVersion(0, 1);
        assertPabloVersion(0, 2);
        assertPabloVersion(0);
    }

    @Test
    public void putWithEndValidTime() {
        submitTx(false, tx -> {
            tx.put(pablo(0), time(1), time(3));
        });

        assertNoPablo(0);
        assertPabloVersion(0, 1);
        assertPabloVersion(0, 2);
        assertNoPablo(3);
        assertNoPablo(4);
        assertNoPablo();
    }

    @Test
    public void putDifferentVersions() {
        submitTx (false,  tx -> {
            tx.put(pablo(0), time(1));
            tx.put(pablo(1), time(3));
        });

        assertNoPablo(0);
        assertPabloVersion(0, 1);
        assertPabloVersion(0, 2);
        assertPabloVersion(1, 3);
        assertPabloVersion(1, 4);
        assertPabloVersion(1);
    }

    @Test
    public void deleteNow() {
        submitTx (false, tx -> {
            tx.put(pablo(0), time(1));
            tx.delete(pabloId);
        });

        assertNoPablo(0);
        assertPabloVersion(0, 1);
        assertPabloVersion(0, 2);
        assertNoPablo();
    }

    @Test
    public void deleteAtSpecificTime() {
        submitTx (false, tx -> {
            tx.put(pablo(0), time(1));
            tx.delete(pabloId, time(3));
        });

        assertNoPablo(0);
        assertPabloVersion(0, 1);
        assertPabloVersion(0, 2);
        assertNoPablo(3);
        assertNoPablo(4);
        assertNoPablo();
    }

    @Test
    public void deleteWithEndTime() {
        submitTx (false, tx -> {
            tx.put(pablo(0), time(1));
            tx.delete(pabloId, time(3), time(5));
        });

        assertNoPablo(0);
        assertPabloVersion(0, 1);
        assertPabloVersion(0, 2);
        assertNoPablo(3);
        assertNoPablo(4);
        assertPabloVersion(0, 5);
        assertPabloVersion(0, 6);
        assertPabloVersion(0);
    }

    @Test
    public void deleteWithSubsequentChange() {
        submitTx (false, tx -> {
            tx.put(pablo(0), time(1));
            tx.put(pablo(1), time(5));
        });

        assertNoPablo(0);
        assertPabloVersion(0, 1);
        assertPabloVersion(0, 2);
        assertPabloVersion(0, 3);
        assertPabloVersion(0, 4);
        assertPabloVersion(1, 5);
        assertPabloVersion(1, 6);
        assertPabloVersion(1);

        submitTx(false, tx -> {
            tx.delete(pabloId, time(3));
        });

        assertNoPablo(0);
        assertPabloVersion(0, 1);
        assertPabloVersion(0, 2);
        assertNoPablo(3);
        assertNoPablo(4);
        assertPabloVersion(1, 5);
        assertPabloVersion(1, 6);
        assertPabloVersion(1);
    }

    @Test
    public void successfulMatchNow() {
        submitTx(false, tx -> {
            tx.put(pablo(0));
        });

        assertPabloVersion(0);

        submitTx(false, tx -> {
            tx.match(pablo(0));
            tx.put(pablo(1));
        });

        assertPabloVersion(1);
    }

    @Test
    public void unsuccessfulMatchNow() {
        submitTx (false, tx -> {
            tx.put(pablo(0));
        });

        assertPabloVersion(0);

        submitTx (true, tx -> {
            tx.match(pablo(2));
            tx.put(pablo(3));
        });

        assertPabloVersion(0);
    }

    @Test
    public void successfulMatchWithValidTime() {
        submitTx (false, tx -> {
            tx.put(pablo(0), time(1), time(3));
        });

        assertNoPablo(0);
        assertPabloVersion(0, 1);
        assertPabloVersion(0, 2);
        assertNoPablo(3);
        assertNoPablo(4);
        assertNoPablo();

        submitTx (false, tx -> {
            tx.match(pablo(0), time(2));
            tx.put(pablo(1), time(3));
        });

        assertNoPablo(0);
        assertPabloVersion(0, 1);
        assertPabloVersion(0, 2);
        assertPabloVersion(1, 3);
        assertPabloVersion(1, 4);
        assertPabloVersion(1);
    }

    @Test
    public void unsuccessfulMatchWithValidTime() {
        submitTx (false, tx -> {
            tx.put(pablo(0), time(1), time(3));
        });

        assertNoPablo(0);
        assertPabloVersion(0, 1);
        assertPabloVersion(0, 2);
        assertNoPablo(3);
        assertNoPablo(4);
        assertNoPablo();

        submitTx (true, tx -> {
            tx.match(pablo(0), time(4));
            tx.put(pablo(1), time(3));
        });

        assertNoPablo(0);
        assertPabloVersion(0, 1);
        assertPabloVersion(0, 2);
        assertNoPablo(3);
        assertNoPablo(4);
        assertNoPablo();
    }

    @Test
    public void successfulEmptyMatch() {
        assertNoPablo();

        submitTx (false,  tx -> {
            tx.matchNotExists(pabloId);
            tx.put(pablo(0));
        });

        assertPabloVersion(0);
    }

    @Test
    public void unsuccessfulEmptyMatch() {
        submitTx (false, tx -> {
            tx.put(pablo(0));
        });

        assertPabloVersion(0);

        submitTx (true, tx -> {
            tx.matchNotExists(pabloId);
            tx.put(pablo(0));
        });

        assertPabloVersion(0);
    }

    @Test
    public void successfulEmptyMatchAtTime() {
        submitTx (false, tx -> {
            tx.put(pablo(0), time(1), time(3));
        });

        assertNoPablo(0);
        assertPabloVersion(0, 1);
        assertPabloVersion(0, 2);
        assertNoPablo(3);
        assertNoPablo(4);
        assertNoPablo();

        submitTx (false, tx -> {
            tx.matchNotExists(pabloId, time(3));
            tx.put(pablo(1), time(3));
        });

        assertNoPablo(0);
        assertPabloVersion(0, 1);
        assertPabloVersion(0, 2);
        assertPabloVersion(1, 3);
        assertPabloVersion(1, 4);
        assertPabloVersion(1);
    }

    @Test
    public void unsuccessfulEmptyMatchAtTime() {
        submitTx (false, tx -> {
            tx.put(pablo(0), time(1), time(3));
        });

        assertNoPablo(0);
        assertPabloVersion(0, 1);
        assertPabloVersion(0, 2);
        assertNoPablo(3);
        assertNoPablo(4);
        assertNoPablo();

        submitTx (true, tx -> {
            tx.matchNotExists(pabloId, time(2));
            tx.put(pablo(1), time(3));
        });

        assertNoPablo(0);
        assertPabloVersion(0, 1);
        assertPabloVersion(0, 2);
        assertNoPablo(3);
        assertNoPablo(4);
        assertNoPablo();
    }

    @Test
    public void reusingTransactions() {
        Transaction transaction = Transaction.buildTx(tx -> {
            tx.delete(pabloId);
        });

        submitTx(false, tx -> {
            tx.put(pablo(0));
        });

        assertPabloVersion(0);

        submitTx(false, transaction);

        assertNoPablo();

        submitTx(false, tx -> {
            tx.put(pablo(1));
        });

        assertPabloVersion(1);

        submitTx(false, transaction);

        assertNoPablo();
    }

    @Test
    public void evictTest() {
        submitTx (false,  tx -> {
            tx.put(pablo(0), time(1));
            tx.put(pablo(1), time(3));
        });

        assertNoPablo(0);
        assertPabloVersion(0, 1);
        assertPabloVersion(0, 2);
        assertPabloVersion(1, 3);
        assertPabloVersion(1, 4);
        assertPabloVersion(1);

        submitTx (false, tx -> {
           tx.evict(pabloId);
        });

        assertNoPablo(0);
        assertNoPablo(1);
        assertNoPablo(2);
        assertNoPablo(3);
        assertNoPablo(4);
        assertNoPablo();
    }

    private void submitTx(boolean shouldAbort, Consumer<Transaction.Builder> f) {
        Transaction transaction = Transaction.buildTx(f);
        submitTx(shouldAbort, transaction);
    }

    /**
     * This will also check that we can successfully rebuild the TransactionInstant from the TxLog
     */
    private void submitTx(boolean shouldAbort, Transaction transaction) {
        TransactionInstant submitted = node.submitTx(transaction);

        awaitTx(node, submitted);

        ICursor<Map<Keyword, ?>> cursor = node.openTxLog(submitted.getId() - 1, true);
        if (shouldAbort) {
            assertFalse(cursor.hasNext());
            close(cursor);
            return;
        }

        assertTrue(cursor.hasNext());
        Map<Keyword, ?> transactionLogEntry = cursor.next();
        assertFalse(cursor.hasNext());

        close(cursor);

        assertNotNull(transactionLogEntry);

        assertEquals(submitted, getTransactionInstant(transactionLogEntry));
    }

    private void assertPabloVersion(int version) {
        assertPabloVersion(version, null);
    }

    private void assertPabloVersion(int version, int timeIndex) {
        assertPabloVersion(version, time(timeIndex));
    }

    private void assertPabloVersion(int version, Date validTime) {
        AbstractCruxDocument fromDb;
        if (validTime == null) {
            fromDb = node.db().entity(pabloId);
        }
        else {
            fromDb = node.db(validTime).entity(pabloId);
        }

        if (fromDb == null) {
            fail();
        }

        assertEquals(pablo(version), fromDb);
    }

    private Date time(int timeIndex) {
        return times.get(timeIndex);
    }

    private PersonDocument pablo(int version) {
        return pablos.get(version);
    }

    private void assertNoPablo() {
        assertNoPablo(null);
    }

    private void assertNoPablo(int timeIndex) {
        assertNoPablo(time(timeIndex));
    }

    private void assertNoPablo(Date validTime) {
        Object result;
        if (validTime == null) {
            result = node.db().entity(pabloId);
        }
        else {
            result = node.db(validTime).entity(pabloId);
        }

        assertNull(result);
    }
}