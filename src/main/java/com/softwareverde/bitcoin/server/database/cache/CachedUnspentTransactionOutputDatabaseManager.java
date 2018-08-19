package com.softwareverde.bitcoin.server.database.cache;

import com.softwareverde.bitcoin.transaction.TransactionId;
import com.softwareverde.bitcoin.transaction.output.TransactionOutputId;
import com.softwareverde.constable.list.List;
import com.softwareverde.database.DatabaseException;
import com.softwareverde.database.Query;
import com.softwareverde.database.Row;
import com.softwareverde.database.mysql.BatchedInClauseQuery;
import com.softwareverde.database.mysql.BatchedInsertQuery;
import com.softwareverde.database.mysql.MysqlDatabaseConnection;
import com.softwareverde.util.Util;

public class CachedUnspentTransactionOutputDatabaseManager {
    public static void loadCache(final MysqlDatabaseConnection databaseConnection) throws DatabaseException {
        databaseConnection.executeSql(
                new Query("INSERT INTO _transaction_outputs (id, transaction_id, `index`) SELECT id, transaction_id, `index` FROM transaction_outputs WHERE is_spent = 0")
        );
    }

    protected final MysqlDatabaseConnection _databaseConnection;

    public CachedUnspentTransactionOutputDatabaseManager(final MysqlDatabaseConnection databaseConnection) {
        _databaseConnection = databaseConnection;
    }

    public TransactionOutputId findCachedUnspentTransactionOutput(final TransactionId transactionId, final Integer transactionOutputIndex) throws DatabaseException {
        final java.util.List<Row> rows = _databaseConnection.query(
            new Query("SELECT id FROM _transaction_outputs WHERE transaction_id = ? AND `index` = ?")
                .setParameter(transactionId)
                .setParameter(transactionOutputIndex)
        );

        if (rows.isEmpty()) { return null; }

        final Row row = rows.get(0);
        return TransactionOutputId.wrap(row.getLong("id"));
    }

    public void markCachedUnspentTransactionOutputAsSpent(final TransactionOutputId transactionOutputId) throws DatabaseException {
        _databaseConnection.executeSql(
            new Query("DELETE FROM _transaction_outputs WHERE id = ?")
                .setParameter(transactionOutputId)
        );
    }

    public void markCachedUnspentTransactionOutputsAsSpent(final List<TransactionOutputId> transactionOutputIds) throws DatabaseException {
        final Query batchedUpdateQuery = new BatchedInClauseQuery("DELETE FROM _transaction_outputs WHERE id IN(?)");
        for (final TransactionOutputId transactionOutputId : transactionOutputIds) {
            batchedUpdateQuery.setParameter(transactionOutputId);
        }

        _databaseConnection.executeSql(batchedUpdateQuery);
    }

    public void insertCachedTransactionOutput(final TransactionOutputId transactionOutputId, final TransactionId transactionId, final Integer transactionOutputIndex) throws DatabaseException {
        _databaseConnection.executeSql(
            new Query("INSERT INTO _transaction_outputs (id, transaction_id, `index`) VALUES (?, ?, ?)")
                .setParameter(transactionOutputId)
                .setParameter(transactionId)
                .setParameter(transactionOutputIndex)
        );
    }

    public void insertCachedTransactionOutputs(final List<TransactionOutputId> transactionOutputIds, final List<TransactionId> transactionIds, final List<Integer> transactionOutputIndexes) throws DatabaseException {
        if (! Util.areEqual(transactionOutputIds.getSize(), transactionIds.getSize())) {
            throw new RuntimeException("TransactionOutputDatabaseManager::_insertCachedTransactionOutputs -- transactionOutputIds.getSize must equal transactionIds.getSize");
        }

        if (! Util.areEqual(transactionOutputIds.getSize(), transactionOutputIndexes.getSize())) {
            throw new RuntimeException("TransactionOutputDatabaseManager::_insertCachedTransactionOutputs -- transactionOutputIds.getSize must equal transactionOutputIndexes.getSize");
        }

        final Query batchInsertQuery = new BatchedInsertQuery("INSERT INTO _transaction_outputs (id, transaction_id, `index`) VALUES (?, ?, ?)");

        final Integer transactionOutputCount = transactionOutputIds.getSize();
        for (int i = 0; i < transactionOutputCount; ++i) {
            final TransactionOutputId transactionOutputId = transactionOutputIds.get(i);
            final TransactionId transactionId = transactionIds.get(i);
            final Integer transactionOutputIndex = transactionOutputIndexes.get(i);

            batchInsertQuery.setParameter(transactionOutputId);
            batchInsertQuery.setParameter(transactionId);
            batchInsertQuery.setParameter(transactionOutputIndex);
        }

        _databaseConnection.executeSql(batchInsertQuery);
    }
}
