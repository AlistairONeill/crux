package crux.api;

import clojure.lang.IPersistentMap;
import clojure.lang.Keyword;
import clojure.lang.LazySeq;
import clojure.lang.PersistentArrayMap;

import java.io.Closeable;
import java.util.Map;
import java.util.function.Consumer;

public abstract class CruxListener implements Closeable {
    private static final Keyword EVENT_TYPE = Keyword.intern("crux/event-type");
    private final Keyword eventType;
    protected AutoCloseable closeable = null;

    private CruxListener(Keyword eventType) {
        this.eventType = eventType;
    }

    public IPersistentMap getOpts() {
        return getSpecificOpts().assoc(EVENT_TYPE, eventType);
    }

    public CruxListener setCloseable(AutoCloseable closeable) {
        this.closeable = closeable;
        return this;
    }

    abstract IPersistentMap getSpecificOpts();
    public abstract void performAction(Map<Keyword, ?> data);

    @Override
    public final void close() {
        if (closeable == null) {
            throw new RuntimeException("Trying to close listener before being opened.");
        }

        try {
            closeable.close();
        }
        catch (Exception e) {
            //This should never throw, but the interface declares it as throwing.
        }

        closeable = null;
    }

    public static class IndexedTxListener extends CruxListener {
        private static final Keyword EVENT_TYPE_INDEXED_TX = Keyword.intern("crux/indexed-tx");
        private static final Keyword WITH_TX_OPS = Keyword.intern("with-tx-ops?");

        private final boolean withOps;
        private final Consumer<Event> action;

        public static class Event {
            private static final Keyword COMMITTED = Keyword.intern("committed?");
            //TODO: Move this when TransactionLogs are handled better!
            private static final Keyword TX_OPS = Keyword.intern("crux/tx-ops");

            private final boolean committed;
            private final TransactionInstant transactionInstant;
            private final LazySeq transactionOperations;

            private static Event factory(Map<Keyword, ?> map) {
                boolean committed = (boolean) map.get(COMMITTED);
                TransactionInstant transactionInstant = TransactionInstant.factory(map);
                LazySeq transactionOperations = (LazySeq) map.get(TX_OPS);
                return new Event(committed, transactionInstant, transactionOperations);
            }

            private Event(boolean committed, TransactionInstant transactionInstant, LazySeq transactionOperations) {
                this.committed = committed;
                this.transactionInstant = transactionInstant;
                this.transactionOperations = transactionOperations;
            }

            public boolean isCommitted() {
                return committed;
            }

            public TransactionInstant getTransactionInstant() {
                return transactionInstant;
            }

            public LazySeq getTransactionOperations() {
                return transactionOperations;
            }
        }

        public IndexedTxListener(boolean withOps, Consumer<Event> action) {
            super(EVENT_TYPE_INDEXED_TX);
            this.withOps = withOps;
            this.action = action;
        }

        @Override
        IPersistentMap getSpecificOpts() {
            return PersistentArrayMap.EMPTY
                    .assoc(WITH_TX_OPS, withOps);
        }

        @Override
        public void performAction(Map<Keyword, ?> data) {
            Event event = Event.factory(data);
            action.accept(event);
        }
    }
}
