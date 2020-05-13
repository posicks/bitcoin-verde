package com.softwareverde.bitcoin.server.module;

import com.softwareverde.bitcoin.CoreInflater;
import com.softwareverde.bitcoin.block.Block;
import com.softwareverde.bitcoin.block.BlockId;
import com.softwareverde.bitcoin.block.header.BlockHeader;
import com.softwareverde.bitcoin.block.validator.BlockValidationResult;
import com.softwareverde.bitcoin.block.validator.BlockValidator;
import com.softwareverde.bitcoin.block.validator.BlockValidatorFactory;
import com.softwareverde.bitcoin.chain.segment.BlockchainSegmentId;
import com.softwareverde.bitcoin.chain.time.MutableMedianBlockTime;
import com.softwareverde.bitcoin.inflater.MasterInflater;
import com.softwareverde.bitcoin.server.Environment;
import com.softwareverde.bitcoin.server.configuration.BitcoinProperties;
import com.softwareverde.bitcoin.server.database.Database;
import com.softwareverde.bitcoin.server.database.DatabaseConnection;
import com.softwareverde.bitcoin.server.database.DatabaseConnectionFactory;
import com.softwareverde.bitcoin.server.database.ReadUncommittedDatabaseConnectionFactoryWrapper;
import com.softwareverde.bitcoin.server.module.node.database.DatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.DatabaseManagerFactory;
import com.softwareverde.bitcoin.server.module.node.database.block.fullnode.FullNodeBlockDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.block.header.BlockHeaderDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.blockchain.BlockchainDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.fullnode.FullNodeDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.fullnode.FullNodeDatabaseManagerFactory;
import com.softwareverde.bitcoin.server.module.node.store.PendingBlockStore;
import com.softwareverde.bitcoin.server.module.node.store.PendingBlockStoreCore;
import com.softwareverde.bitcoin.transaction.validator.LazyLoadingMedianBlockTimeSet;
import com.softwareverde.bitcoin.transaction.validator.MedianBlockTimeSet;
import com.softwareverde.bitcoin.transaction.validator.TransactionValidatorFactory;
import com.softwareverde.bitcoin.util.BitcoinUtil;
import com.softwareverde.bitcoin.util.StringUtil;
import com.softwareverde.database.DatabaseException;
import com.softwareverde.database.mysql.connection.ReadUncommittedDatabaseConnectionFactory;
import com.softwareverde.logging.Logger;
import com.softwareverde.network.time.MutableNetworkTime;
import com.softwareverde.security.hash.sha256.Sha256Hash;
import com.softwareverde.util.Util;
import com.softwareverde.util.timer.MilliTimer;

public class ChainValidationModule {
    protected final BitcoinProperties _bitcoinProperties;
    protected final Environment _environment;
    protected final BlockValidatorFactory _blockValidatorFactory;
    protected final Sha256Hash _startingBlockHash;
    protected final PendingBlockStore _blockStore;

    public ChainValidationModule(final BitcoinProperties bitcoinProperties, final Environment environment, final String startingBlockHash) {
        _bitcoinProperties = bitcoinProperties;
        _environment = environment;

        final MasterInflater masterInflater = new CoreInflater();
        final MutableNetworkTime mutableNetworkTime = new MutableNetworkTime();
        final MutableMedianBlockTime medianBlockTime;
        final DatabaseManagerFactory databaseManagerFactory;
        { // Initialize MedianBlockTime...
            final Database database = _environment.getDatabase();

            databaseManagerFactory = new FullNodeDatabaseManagerFactory(database.newConnectionFactory(), null, masterInflater);
            {
                MutableMedianBlockTime newMedianBlockTime = null;
                try (final DatabaseManager databaseManager = databaseManagerFactory.newDatabaseManager()) {
                    final BlockHeaderDatabaseManager blockHeaderDatabaseManager = databaseManager.getBlockHeaderDatabaseManager();
                    newMedianBlockTime = blockHeaderDatabaseManager.initializeMedianBlockTime();
                }
                catch (final DatabaseException exception) {
                    Logger.error(exception);
                    BitcoinUtil.exitFailure();
                }
                medianBlockTime = newMedianBlockTime;
            }
        }


        final MedianBlockTimeSet medianBlockTimeSet = new LazyLoadingMedianBlockTimeSet(databaseManagerFactory);
        final TransactionValidatorFactory transactionValidatorFactory = new TransactionValidatorFactory(mutableNetworkTime, medianBlockTime, medianBlockTimeSet);
        _blockValidatorFactory = new BlockValidatorFactory(transactionValidatorFactory, mutableNetworkTime, medianBlockTime);

        _startingBlockHash = Util.coalesce(Sha256Hash.fromHexString(startingBlockHash), BlockHeader.GENESIS_BLOCK_HASH);

        { // Initialize the BlockCache...
            if (bitcoinProperties.isBlockCacheEnabled()) {
                final String blockCacheDirectory = (bitcoinProperties.getDataDirectory() + "/" + BitcoinProperties.DATA_CACHE_DIRECTORY_NAME + "/blocks");
                final String pendingBlockCacheDirectory = (bitcoinProperties.getDataDirectory() + "/" + BitcoinProperties.DATA_CACHE_DIRECTORY_NAME + "/pending-blocks");
                _blockStore = new PendingBlockStoreCore(blockCacheDirectory, pendingBlockCacheDirectory, masterInflater);
            }
            else {
                _blockStore = null;
            }
        }
    }

    public void run() {
        final Thread mainThread = Thread.currentThread();
        mainThread.setPriority(Thread.MAX_PRIORITY);

        final Database database = _environment.getDatabase();
        // final MasterDatabaseManagerCache masterDatabaseManagerCache = _environment.getMasterDatabaseManagerCache();

        final MasterInflater masterInflater = new CoreInflater();

        Sha256Hash nextBlockHash = _startingBlockHash;
        try (final DatabaseConnection databaseConnection = database.newConnection();) {
            final FullNodeDatabaseManager databaseManager = new FullNodeDatabaseManager(
                databaseConnection,
                _blockStore,
                masterInflater,
                _bitcoinProperties.getMaxCachedUtxoCount(),
                _bitcoinProperties.getUtxoCachePurgePercent()
            );

            final BlockchainDatabaseManager blockchainDatabaseManager = databaseManager.getBlockchainDatabaseManager();
            final BlockHeaderDatabaseManager blockHeaderDatabaseManager = databaseManager.getBlockHeaderDatabaseManager();
            final FullNodeBlockDatabaseManager blockDatabaseManager = databaseManager.getBlockDatabaseManager();

            final DatabaseConnectionFactory databaseConnectionFactory = database.newConnectionFactory();
            final ReadUncommittedDatabaseConnectionFactory readUncommittedDatabaseConnectionFactory = new ReadUncommittedDatabaseConnectionFactoryWrapper(databaseConnectionFactory);
            final FullNodeDatabaseManagerFactory databaseManagerFactory = new FullNodeDatabaseManagerFactory(readUncommittedDatabaseConnectionFactory, _blockStore, masterInflater);

            final BlockValidator blockValidator = _blockValidatorFactory.newBlockValidator(databaseManagerFactory);
            blockValidator.setMaxThreadCount(_bitcoinProperties.getMaxThreadCount());
            blockValidator.setShouldLogValidBlocks(true);
            blockValidator.setTrustedBlockHeight(BlockValidator.DO_NOT_TRUST_BLOCKS);

            final BlockchainSegmentId headBlockchainSegmentId = blockchainDatabaseManager.getHeadBlockchainSegmentId();

            final BlockId headBlockId = blockDatabaseManager.getHeadBlockId();
            final Long maxBlockHeight = blockHeaderDatabaseManager.getBlockHeight(headBlockId);

            Long validatedTransactionCount = 0L;
            final Long startTime = System.currentTimeMillis();
            while (true) {
                final Sha256Hash blockHash = nextBlockHash;

                final BlockId blockId = blockHeaderDatabaseManager.getBlockHeaderId(nextBlockHash);
                final Long blockHeight = blockHeaderDatabaseManager.getBlockHeight(blockId);

                final int percentComplete = (int) ((blockHeight * 100) / maxBlockHeight.floatValue());

                if (blockHeight % (maxBlockHeight / 100) == 0) {
                    final Integer secondsElapsed;
                    final Float blocksPerSecond;
                    final Float transactionsPerSecond;
                    {
                        final Long now = System.currentTimeMillis();
                        final Integer seconds = (int) ((now - startTime) / 1000L);
                        final Long blockCount = blockHeight;
                        blocksPerSecond = (blockCount / (seconds.floatValue() + 1));
                        secondsElapsed = seconds;
                        transactionsPerSecond = (validatedTransactionCount / (seconds.floatValue() + 1));
                    }

                    Logger.info(percentComplete + "% complete. " + blockHeight + " of " + maxBlockHeight + " - " + blockHash + " ("+ String.format("%.2f", blocksPerSecond) +" bps) (" + String.format("%.2f", transactionsPerSecond) + " tps) ("+ StringUtil.formatNumberString(secondsElapsed) +" seconds)");
                }

                final MilliTimer blockInflaterTimer = new MilliTimer();
                blockInflaterTimer.start();
                final Boolean blockIsCached;
                final Block block;
                {
                    Block cachedBlock = null;
                    if (_blockStore != null) {
                        cachedBlock = _blockStore.getBlock(blockHash, blockHeight);
                    }

                    if (cachedBlock != null) {
                        block = cachedBlock;
                        blockIsCached = true;
                    }
                    else {
                        block = blockDatabaseManager.getBlock(blockId);
                        blockIsCached = false;
                    }
                }
                blockInflaterTimer.stop();
                System.out.println("Block Inflation: " +  block.getHash() + " " + blockInflaterTimer.getMillisecondsElapsed() + "ms");

                validatedTransactionCount += blockDatabaseManager.getTransactionCount(blockId);
                final BlockValidationResult blockValidationResult = blockValidator.validateBlock(blockId, block);

                if (! blockValidationResult.isValid) {
                    Logger.error("Invalid block found: " + blockHash + "(" + blockValidationResult.errorMessage + ")");
                    break;
                }

                if ( (! blockIsCached) && (_blockStore != null) ) {
                    _blockStore.storeBlock(block, blockHeight);
                }

                nextBlockHash = null;
                final BlockId nextBlockId = blockHeaderDatabaseManager.getChildBlockId(headBlockchainSegmentId, blockId);
                if (nextBlockId != null) {
                    final Boolean nextBlockHasTransactions = blockDatabaseManager.hasTransactions(nextBlockId);
                    if (nextBlockHasTransactions) {
                        nextBlockHash = blockHeaderDatabaseManager.getBlockHash(nextBlockId);
                    }
                }
            }
        }
        catch (final DatabaseException exception) {
            Logger.error("Last validated block: " + nextBlockHash, exception);
            BitcoinUtil.exitFailure();
        }

        System.exit(0);
    }
}
