package com.softwareverde.bitcoin.test;

import com.softwareverde.bitcoin.server.database.BlockChainDatabaseManager;
import com.softwareverde.bitcoin.server.database.TransactionDatabaseManager;
import com.softwareverde.bitcoin.server.database.cache.BlockChainSegmentCache;
import com.softwareverde.bitcoin.server.database.cache.TransactionCache;
import com.softwareverde.database.mysql.embedded.DatabaseInitializer;
import com.softwareverde.test.database.MysqlTestDatabase;
import com.softwareverde.test.util.TestUtil;

public class IntegrationTest {
    protected static final MysqlTestDatabase _database = new MysqlTestDatabase();
    static {
        _resetDatabase();
        _resetCache();
    }

    protected static void _resetDatabase() {
        final DatabaseInitializer databaseInitializer = new DatabaseInitializer("queries/init.sql", 1, new DatabaseInitializer.DatabaseUpgradeHandler() {
            @Override
            public Boolean onUpgrade(final int i, final int i1) { return false; }
        });
        try {
            _database.reset();
            databaseInitializer.initializeDatabase(_database.getDatabaseInstance(), _database.getDatabaseConnectionFactory(), _database.getCredentials());
        }
        catch (final Exception exception) {
            throw new RuntimeException(exception);
        }
    }

    protected static void _resetCache() {
        final TransactionCache transactionCache = TestUtil.getStaticValue(TransactionDatabaseManager.class, "TRANSACTION_CACHE");
        transactionCache.clear();

        final BlockChainSegmentCache blockChainSegmentCache = TestUtil.getStaticValue(BlockChainDatabaseManager.class, "BLOCK_CHAIN_SEGMENT_CACHE");
        blockChainSegmentCache.clear();
    }
}
