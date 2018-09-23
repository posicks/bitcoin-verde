package com.softwareverde.bitcoin.server.database.cache;

import com.softwareverde.bitcoin.address.AddressId;
import com.softwareverde.bitcoin.block.BlockId;
import com.softwareverde.bitcoin.chain.segment.BlockChainSegmentId;
import com.softwareverde.bitcoin.transaction.ImmutableTransaction;
import com.softwareverde.bitcoin.transaction.Transaction;
import com.softwareverde.bitcoin.transaction.TransactionId;
import com.softwareverde.bitcoin.transaction.output.TransactionOutputId;
import com.softwareverde.bitcoin.type.hash.sha256.ImmutableSha256Hash;

public class LocalDatabaseManagerCache implements DatabaseManagerCache {

    public LocalDatabaseManagerCache() { }

    public LocalDatabaseManagerCache(final MasterDatabaseManagerCache masterCache) {
        _transactionIdCache.setMasterCache(masterCache.getTransactionIdCache());
        _transactionCache.setMasterCache(masterCache.getTransactionCache());
        _transactionOutputIdCache.setMasterCache(masterCache.getTransactionOutputIdCache());
        _blockIdBlockChainSegmentIdCache.setMasterCache(masterCache.getBlockIdBlockChainSegmentIdCache());
        _addressIdCache.setMasterCache(masterCache.getAddressIdCache());
    }

    @Override
    public void log() {
        _transactionIdCache.debug();
        _transactionCache.debug();
        _transactionOutputIdCache.debug();
        _blockIdBlockChainSegmentIdCache.debug();
        _addressIdCache.debug();
    }

    @Override
    public void resetLog() {
        _transactionIdCache.resetDebug();
        _transactionCache.resetDebug();
        _transactionOutputIdCache.resetDebug();
        _blockIdBlockChainSegmentIdCache.resetDebug();
        _addressIdCache.resetDebug();
    }


    // TRANSACTION ID CACHE --------------------------------------------------------------------------------------------

    protected final HashMapCache<ImmutableSha256Hash, TransactionId> _transactionIdCache = new HashMapCache<ImmutableSha256Hash, TransactionId>("TransactionIdCache", HashMapCache.DEFAULT_CACHE_SIZE);

    @Override
    public void cacheTransactionId(final ImmutableSha256Hash transactionHash, final TransactionId transactionId) {
        _transactionIdCache.cacheItem(transactionHash, transactionId);
    }

    @Override
    public TransactionId getCachedTransactionId(final ImmutableSha256Hash transactionHash) {
        return _transactionIdCache.getCachedItem(transactionHash);
    }

    @Override
    public void invalidateTransactionIdCache() {
        _transactionIdCache.invalidate();
    }

    public HashMapCache<ImmutableSha256Hash, TransactionId> getTransactionIdCache() { return _transactionIdCache; }

    // -----------------------------------------------------------------------------------------------------------------


    // TRANSACTION CACHE -----------------------------------------------------------------------------------------------

    protected final HashMapCache<TransactionId, ImmutableTransaction> _transactionCache = new HashMapCache<TransactionId, ImmutableTransaction>("TransactionCache", HashMapCache.DEFAULT_CACHE_SIZE);

    @Override
    public void cacheTransaction(final TransactionId transactionId, final ImmutableTransaction transaction) {
        _transactionCache.cacheItem(transactionId, transaction);
    }

    @Override
    public Transaction getCachedTransaction(final TransactionId transactionId) {
        return _transactionCache.getCachedItem(transactionId);
    }

    @Override
    public void invalidateTransactionCache() {
        _transactionCache.invalidate();
    }

    public HashMapCache<TransactionId, ImmutableTransaction> getTransactionCache() { return _transactionCache; }

    // -----------------------------------------------------------------------------------------------------------------

    // TRANSACTION OUTPUT ID CACHE -------------------------------------------------------------------------------------

    protected final HashMapCache<CachedTransactionOutputIdentifier, TransactionOutputId> _transactionOutputIdCache = new HashMapCache<CachedTransactionOutputIdentifier, TransactionOutputId>("TransactionOutputId", HashMapCache.DEFAULT_CACHE_SIZE);

    @Override
    public void cacheTransactionOutputId(final TransactionId transactionId, final Integer transactionOutputIndex, final TransactionOutputId transactionOutputId) {
        final CachedTransactionOutputIdentifier cachedTransactionOutputIdentifier = new CachedTransactionOutputIdentifier(transactionId, transactionOutputIndex);
        _transactionOutputIdCache.cacheItem(cachedTransactionOutputIdentifier, transactionOutputId);
    }

    @Override
    public TransactionOutputId getCachedTransactionOutputId(final TransactionId transactionId, final Integer transactionOutputIndex) {
        final CachedTransactionOutputIdentifier cachedTransactionOutputIdentifier = new CachedTransactionOutputIdentifier(transactionId, transactionOutputIndex);
        return _transactionOutputIdCache.getCachedItem(cachedTransactionOutputIdentifier);
    }

    @Override
    public void invalidateTransactionOutputIdCache() {
        _transactionOutputIdCache.invalidate();
    }

    public HashMapCache<CachedTransactionOutputIdentifier, TransactionOutputId> getTransactionOutputIdCache() { return _transactionOutputIdCache; }

    // -----------------------------------------------------------------------------------------------------------------

    // BLOCK BLOCK CHAIN SEGMENT ID CACHE ------------------------------------------------------------------------------

    protected final HashMapCache<BlockId, BlockChainSegmentId> _blockIdBlockChainSegmentIdCache = new HashMapCache<BlockId, BlockChainSegmentId>("BlockId-BlockChainSegmentId", 1460);

    @Override
    public void cacheBlockChainSegmentId(final BlockId blockId, final BlockChainSegmentId blockChainSegmentId) {
        _blockIdBlockChainSegmentIdCache.cacheItem(blockId, blockChainSegmentId);
    }

    @Override
    public BlockChainSegmentId getCachedBlockChainSegmentId(final BlockId blockId) {
        return _blockIdBlockChainSegmentIdCache.getCachedItem(blockId);
    }

    @Override
    public void invalidateBlockIdBlockChainSegmentIdCache() {
        _blockIdBlockChainSegmentIdCache.invalidate();
    }

    public HashMapCache<BlockId, BlockChainSegmentId> getBlockIdBlockChainSegmentIdCache() { return _blockIdBlockChainSegmentIdCache; }

    // -----------------------------------------------------------------------------------------------------------------

    // ADDRESS ID CACHE ------------------------------------------------------------------------------------------------

    protected final HashMapCache<String, AddressId> _addressIdCache = new HashMapCache<String, AddressId>("AddressId", 262144);

    @Override
    public void cacheAddressId(final String address, final AddressId addressId) {
        _addressIdCache.cacheItem(address, addressId);
    }

    @Override
    public AddressId getCachedAddressId(final String address) {
        return _addressIdCache.getCachedItem(address);
    }

    @Override
    public void invalidateAddressIdCache() {
        _addressIdCache.invalidate();
    }

    public HashMapCache<String, AddressId> getAddressIdCache() { return _addressIdCache; }

    // -----------------------------------------------------------------------------------------------------------------
}