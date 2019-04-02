package com.softwareverde.bitcoin.server.module;

import com.softwareverde.bitcoin.block.Block;
import com.softwareverde.bitcoin.block.BlockDeflater;
import com.softwareverde.bitcoin.block.BlockId;
import com.softwareverde.bitcoin.block.header.BlockHeader;
import com.softwareverde.bitcoin.chain.segment.BlockchainSegmentId;
import com.softwareverde.bitcoin.chain.time.MutableMedianBlockTime;
import com.softwareverde.bitcoin.hash.sha256.Sha256Hash;
import com.softwareverde.bitcoin.server.Configuration;
import com.softwareverde.bitcoin.server.Environment;
import com.softwareverde.bitcoin.server.database.BitcoinVerdeDatabase;
import com.softwareverde.bitcoin.server.database.Database;
import com.softwareverde.bitcoin.server.database.DatabaseConnection;
import com.softwareverde.bitcoin.server.database.cache.DatabaseManagerCache;
import com.softwareverde.bitcoin.server.database.cache.LocalDatabaseManagerCache;
import com.softwareverde.bitcoin.server.database.cache.MasterDatabaseManagerCache;
import com.softwareverde.bitcoin.server.database.cache.utxo.NativeUnspentTransactionOutputCache;
import com.softwareverde.bitcoin.server.database.cache.utxo.UnspentTransactionOutputCache;
import com.softwareverde.bitcoin.server.module.node.database.BlockDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.BlockHeaderDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.BlockchainDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.TransactionDatabaseManager;
import com.softwareverde.bitcoin.util.BitcoinUtil;
import com.softwareverde.bitcoin.util.StringUtil;
import com.softwareverde.database.mysql.embedded.factory.ReadUncommittedDatabaseConnectionFactory;
import com.softwareverde.io.Logger;
import com.softwareverde.network.time.MutableNetworkTime;
import com.softwareverde.network.time.NetworkTime;
import com.softwareverde.util.Util;

import java.io.File;
import java.io.FileOutputStream;

public class DatafileModule {
    public static void execute(final String configurationFileName, final String startingBlockHash) {
        final DatafileModule datafileModule = new DatafileModule(configurationFileName, startingBlockHash);
        datafileModule.run();
    }

    protected final Configuration _configuration;
    protected final Environment _environment;
    protected final Sha256Hash _startingBlockHash;

    protected Configuration _loadConfigurationFile(final String configurationFilename) {
        final File configurationFile =  new File(configurationFilename);
        if (! configurationFile.isFile()) {
            Logger.error("Invalid configuration file.");
            BitcoinUtil.exitFailure();
        }

        return new Configuration(configurationFile);
    }

    protected DatafileModule(final String configurationFilename, final String startingBlockHash) {
        _configuration = _loadConfigurationFile(configurationFilename);

        _startingBlockHash = Util.coalesce(Sha256Hash.fromHexString(startingBlockHash), BlockHeader.GENESIS_BLOCK_HASH);

        final Configuration.BitcoinProperties bitcoinProperties = _configuration.getBitcoinProperties();
        final Configuration.DatabaseProperties databaseProperties = bitcoinProperties.getDatabaseProperties();
        databaseProperties.setUseEmbeddedDatabase(false);

        final Database database = BitcoinVerdeDatabase.newInstance(BitcoinVerdeDatabase.BITCOIN, databaseProperties);
        if (database == null) {
            Logger.log("Error initializing database.");
            BitcoinUtil.exitFailure();
        }
        Logger.log("[Database Online]");

        { // Initialize the NativeUnspentTransactionOutputCache...
            final Boolean nativeCacheIsEnabled = NativeUnspentTransactionOutputCache.isEnabled();
            if (nativeCacheIsEnabled) {
                NativeUnspentTransactionOutputCache.init();
            }
            else {
                Logger.log("NOTICE: NativeUtxoCache not enabled.");
            }
        }


        final Long maxUtxoCacheByteCount = bitcoinProperties.getMaxUtxoCacheByteCount();
        _environment = new Environment(database, new MasterDatabaseManagerCache(maxUtxoCacheByteCount));
    }

    public void run() {
        final Database database = _environment.getDatabase();
        final MasterDatabaseManagerCache masterDatabaseManagerCache = _environment.getMasterDatabaseManagerCache();
        final UnspentTransactionOutputCache unspentTransactionOutputCache = masterDatabaseManagerCache.getUnspentTransactionOutputCache();

        final Configuration.BitcoinProperties bitcoinProperties = _configuration.getBitcoinProperties();

        Sha256Hash nextBlockHash = _startingBlockHash;
        try (final DatabaseConnection databaseConnection = database.newConnection();
             final DatabaseManagerCache databaseManagerCache = new LocalDatabaseManagerCache(masterDatabaseManagerCache);
             final FileOutputStream manifestOutputStream = new FileOutputStream("blocks/MANIFEST")) {

            final BlockchainDatabaseManager blockchainDatabaseManager = new BlockchainDatabaseManager(databaseConnection, databaseManagerCache);
            final BlockHeaderDatabaseManager blockHeaderDatabaseManager = new BlockHeaderDatabaseManager(databaseConnection, databaseManagerCache);
            final BlockDatabaseManager blockDatabaseManager = new BlockDatabaseManager(databaseConnection, databaseManagerCache);
            final TransactionDatabaseManager transactionDatabaseManager = new TransactionDatabaseManager(databaseConnection, databaseManagerCache);

            final ReadUncommittedDatabaseConnectionFactory databaseConnectionFactory = new ReadUncommittedDatabaseConnectionFactory(database.newConnectionFactory());
            final NetworkTime networkTime = new MutableNetworkTime();
            final MutableMedianBlockTime medianBlockTime = blockHeaderDatabaseManager.initializeMedianBlockTime();

            final BlockchainSegmentId headBlockchainSegmentId = blockchainDatabaseManager.getHeadBlockchainSegmentId();

            final BlockId headBlockId = blockDatabaseManager.getHeadBlockId();
            final Long maxBlockHeight = blockHeaderDatabaseManager.getBlockHeight(headBlockId);

            final BlockDeflater blockDeflater = new BlockDeflater();

            Long validatedTransactionCount = 0L;
            final Long startTime = System.currentTimeMillis();
            while (true) {
                final Sha256Hash blockHash = nextBlockHash;

                final BlockId blockId = blockHeaderDatabaseManager.getBlockHeaderId(nextBlockHash);
                final Long blockHeight = blockHeaderDatabaseManager.getBlockHeight(blockId);

                final Integer percentComplete = (int) ((blockHeight * 100) / maxBlockHeight.floatValue());

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

                    Logger.log(percentComplete + "% complete. " + blockHeight + " of " + maxBlockHeight + " - " + blockHash + " ("+ String.format("%.2f", blocksPerSecond) +" bps) (" + String.format("%.2f", transactionsPerSecond) + " tps) ("+ StringUtil.formatNumberString(secondsElapsed) +" seconds)");
                }

                final Block block = blockDatabaseManager.getBlock(blockId, true);
                validatedTransactionCount += block.getTransactionCount();

                try (final FileOutputStream fileOutputStream = new FileOutputStream("blocks/" + block.getHash())) {
                    fileOutputStream.write(blockDeflater.toBytes(block).getBytes());
                }

                manifestOutputStream.write(block.getHash().getBytes());

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
        catch (final Exception exception) {
            Logger.log(exception);
            Logger.log("Last validated block: " + nextBlockHash);
            BitcoinUtil.exitFailure();
        }

        _environment.getMasterDatabaseManagerCache().close();

        System.exit(0);
    }
}
