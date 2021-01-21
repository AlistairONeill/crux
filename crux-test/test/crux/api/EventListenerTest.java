package crux.api;

import org.junit.*;

import crux.api.*;
import crux.api.tx.*;

import java.util.function.Consumer;

import static crux.api.TestUtils.*;
import static org.junit.Assert.*;

public class EventListenerTest {
    private static ICruxAPI node;
    private static final AbstractCruxDocument document = CruxDocument.create("foo");
    private static final AbstractCruxDocument notMatchedDocument = CruxDocument.create("bar");

    private static class Wrapper<T> {
        private T object = null;

        public T get() {
            return object;
        }

        public void set(T object) {
            this.object = object;
        }
    }

    @BeforeClass
    public static void beforeClass() {
        node = Crux.startNode();
    }

    @AfterClass
    public static void afterClass() {
        close(node);
        node = null;
    }

    @Test
    public void indexedTxWithOps() {
        final Wrapper<CruxListener.IndexedTxListener.Event> wrapper = new Wrapper<>();
        CruxListener listener = new CruxListener.IndexedTxListener(true, event -> {
            wrapper.set(event);
        });

        CruxListener returned = node.listen(listener);

        assertEquals(listener, returned);

        TransactionInstant transactionInstant = submitWithSleep(tx -> {
            tx.put(document);
        });

        CruxListener.IndexedTxListener.Event event = wrapper.get();

        assertNotNull(event);

        assertTrue(event.isCommitted());
        assertEquals(transactionInstant, event.getTransactionInstant());
        assertNotNull(event.getTransactionOperations());
    }

    @Test
    public void indexedTxWithoutOps() {
        final Wrapper<CruxListener.IndexedTxListener.Event> wrapper = new Wrapper<>();
        CruxListener listener = new CruxListener.IndexedTxListener(false, event -> {
            wrapper.set(event);
        });

        CruxListener returned = node.listen(listener);

        assertEquals(listener, returned);

        TransactionInstant transactionInstant = submitWithSleep(tx -> {
            tx.put(document);
        });

        CruxListener.IndexedTxListener.Event event = wrapper.get();

        assertNotNull(event);

        assertTrue(event.isCommitted());
        assertEquals(transactionInstant, event.getTransactionInstant());
        assertNull(event.getTransactionOperations());
    }

    @Test
    public void indexedTxNotCommitted() {
        final Wrapper<CruxListener.IndexedTxListener.Event> wrapper = new Wrapper<>();
        CruxListener listener = new CruxListener.IndexedTxListener(false, event -> {
            wrapper.set(event);
        });

        CruxListener returned = node.listen(listener);

        assertEquals(listener, returned);

        TransactionInstant transactionInstant = submitWithSleep(tx -> {
            tx.match(notMatchedDocument);
        });

        CruxListener.IndexedTxListener.Event event = wrapper.get();

        assertNotNull(event);

        assertFalse(event.isCommitted());
        assertEquals(transactionInstant, event.getTransactionInstant());
        assertNull(event.getTransactionOperations());
    }

    @Test
    public void closingListenerWorksAsIntended() {
        final Wrapper<CruxListener.IndexedTxListener.Event> wrapper = new Wrapper<>();
        CruxListener listener = new CruxListener.IndexedTxListener(false, event -> {
            wrapper.set(event);
        });

        CruxListener returned = node.listen(listener);

        assertEquals(listener, returned);

        submitWithSleep(tx -> {
            tx.put(document);
        });

        CruxListener.IndexedTxListener.Event event = wrapper.get();

        assertNotNull(event);

        wrapper.set(null);
        close(listener);

        submitWithSleep(tx -> {
            tx.put(document);
        });

        event = wrapper.get();

        assertNull(event);
    }

    private TransactionInstant submitWithSleep(Consumer<Transaction.Builder> consumer) {
        TransactionInstant transactionInstant = node.submitTx(Transaction.buildTx(consumer));
        awaitTx(node, transactionInstant);
        sleep(100);
        return transactionInstant;
    }
}
