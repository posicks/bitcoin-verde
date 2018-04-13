package com.softwareverde.bitcoin;


import com.softwareverde.bitcoin.type.hash.Hash;
import com.softwareverde.bitcoin.type.hash.MutableHash;
import com.softwareverde.bitcoin.util.BitcoinUtil;
import com.softwareverde.bitcoin.util.ByteUtil;
import com.softwareverde.bitcoin.util.bytearray.ByteArrayBuilder;
import com.softwareverde.io.Logger;
import com.softwareverde.util.Util;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class SelfishMiningSimulation {
    static final Object mutex = new Object();
    static final LinkedList<MiniBlock> foundBlocks = new LinkedList<MiniBlock>();
    static final LinkedList<Miner> miners = new LinkedList<Miner>();
    static {
        final MiniBlock genesisMiniBlock = new MiniBlock(new MutableHash(), 0L, -1, new MutableHash());
        foundBlocks.add(genesisMiniBlock);
    }

    static volatile long estimatedTimePerBlock = 0L;

    protected Map<Integer, Integer> _calculateMinerCounts() {
        final Map<Integer, Integer> minerCounts = new HashMap<Integer, Integer>();
        synchronized (mutex) {
            for (final MiniBlock miniBlock : foundBlocks) {
                final int blockCount = ( Util.coalesce(minerCounts.get(miniBlock.minerId)) + 1 );
                minerCounts.put(miniBlock.minerId, blockCount);
            }
        }
        return minerCounts;
    }

    protected void _printMinerCounts(final Map<Integer, Integer> minerCounts) {
        for (final Integer minerId : minerCounts.keySet()) {
            if (minerId < 0) { continue; }

            final Integer blockCount = minerCounts.get(minerId);
            Logger.log("Miner "+ minerId + " found " + blockCount);
        }
    }

    @Test
    public void simulateSelfishMining() {
        final int minerCount = 3;

        for (int i=0; i<minerCount-1; ++i) {
            miners.add(new Miner(i));
        }
        miners.add(new SelfishMiner(minerCount-1));

        for (int i=0; i<minerCount; ++i) {
            miners.get(i).startMining();
        }

        int previousBlockCount = 0;
        long lastBlockTime = System.currentTimeMillis();
        int foundBlocksCount = foundBlocks.size();
        while (foundBlocksCount < 10000) {
            try { Thread.sleep(100); } catch (final Exception e) { }
            if (previousBlockCount != foundBlocksCount) {
                final long now = System.currentTimeMillis();
                estimatedTimePerBlock = ((previousBlockCount * estimatedTimePerBlock) + (now - lastBlockTime)) / (previousBlockCount + 1);
                lastBlockTime = now;
                previousBlockCount = foundBlocksCount;

                Logger.log("Block Count: "+ foundBlocksCount + " | Time Per Block: " + estimatedTimePerBlock);

                final Map<Integer, Integer> minerCounts = _calculateMinerCounts();
                _printMinerCounts(minerCounts);
            }

            foundBlocksCount = foundBlocks.size();
        }

        for (int i=0; i<minerCount; ++i) {
            miners.get(i).stopMining();
        }

        Logger.log("Shutting down.");
        for (int i=0; i<minerCount; ++i) {
            miners.get(i).join();
        }

        final Map<Integer, Integer> minerCounts = _calculateMinerCounts();
        _printMinerCounts(minerCounts);
    }
}

class MiniBlock {
    private static final int DIFFICULTY = 3;
    private static final byte ZERO = 0x00;

    public static Hash calculateBlockHash(final Long nonce, final Integer minerId, final Hash previousBlockHash) {
        final ByteArrayBuilder byteArrayBuilder = new ByteArrayBuilder();
        byteArrayBuilder.appendBytes(ByteUtil.longToBytes(nonce));
        byteArrayBuilder.appendBytes(ByteUtil.integerToBytes(minerId));
        byteArrayBuilder.appendBytes(previousBlockHash);
        final byte[] bytes = byteArrayBuilder.build();
        return MutableHash.wrap(BitcoinUtil.sha256(bytes));
    }

    public static boolean isValidBlockHash(final Hash blockHash) {
        for (int i=0; i<DIFFICULTY; ++i) {
            if (blockHash.getByte(i) != ZERO) {
                return false;
            }
        }
        if ((char) blockHash.getByte(DIFFICULTY) > (char) 0xC0) {
            return false;
        }
        return true;
    }

    public final Long nonce;
    public final Hash blockHash;
    public final Hash previousBlockHash;
    public final Integer minerId;

    public MiniBlock(final Hash blockHash, final Long nonce, final Integer minerId, final Hash previousBlockHash) {
        this.nonce = nonce;
        this.blockHash = blockHash;
        this.previousBlockHash = previousBlockHash;
        this.minerId = minerId;
    }
}

class Miner {
    protected final int _minerId;
    protected volatile boolean _shouldContinueMining = true;
    protected volatile boolean _shouldRestart = false;

    protected final Thread _miningThread;

    protected void _foundValidBlock(final MiniBlock newBlock) {
        _broadcastBlock(newBlock);
    }

    protected MiniBlock _getNewestBlock() {
        synchronized (SelfishMiningSimulation.mutex) {
            return SelfishMiningSimulation.foundBlocks.getLast();
        }
    }

    public Miner(final int minerId) {
        _minerId = minerId;

        _miningThread = new Thread(new Runnable() {
            @Override
            public void run() {
                _shouldContinueMining = true;

                while (_shouldContinueMining) {
                    final MiniBlock previousBlock = _getNewestBlock();
                    Logger.log(_minerId + " has started mining after: "+ previousBlock.blockHash);

                    long nonce = 0;
                    while ( (! _shouldRestart) && (_shouldContinueMining) ) {
                        final Hash blockHash = MiniBlock.calculateBlockHash(nonce, _minerId, previousBlock.blockHash);
                        final boolean isValidBlock = MiniBlock.isValidBlockHash(blockHash);
                        if (isValidBlock) {
                            Logger.log("Block Found: "+ blockHash + " by " + _minerId);
                            final MiniBlock newBlock = new MiniBlock(blockHash, nonce, _minerId, previousBlock.blockHash);
                            _foundValidBlock(newBlock);
                            break;
                        }
                        nonce += 1;
                    }

                    _shouldRestart = false;
                    Logger.log(_minerId + " restarting at: "+ System.currentTimeMillis());
                }
            }
        });
    }

    protected void _broadcastBlock(final MiniBlock newMiniBlock) {
        synchronized (SelfishMiningSimulation.mutex) {
            final MiniBlock latestBlock = SelfishMiningSimulation.foundBlocks.getLast();
            if (newMiniBlock.previousBlockHash.equals(latestBlock.blockHash)) {
                SelfishMiningSimulation.foundBlocks.add(newMiniBlock);

                for (final Miner miner : SelfishMiningSimulation.miners) {
                    if (miner == this) { continue; }

                    miner.onBlockMined();
                }
            }
            else {
                Logger.log("!!!! Discarding invalid block from miner " + newMiniBlock.minerId + " (Selfish Miner punished)");
            }
        }
    }

    public synchronized void onBlockMined() {
        _shouldRestart = true;
    }

    public void startMining() {
        if (! _miningThread.isAlive()) {
            _miningThread.start();
        }
    }

    public void stopMining() {
        _shouldContinueMining = false;
    }

    public void join() {
        try { _miningThread.join(); } catch (final Exception exception) { }
    }
}

class SelfishMiner extends Miner {
    protected long _timeLastBlockMined = System.currentTimeMillis();
    protected MiniBlock _secretBlock = null;

    public SelfishMiner(final int minerId) {
        super(minerId);
    }

    @Override
    protected void _foundValidBlock(final MiniBlock newBlock) {
        _secretBlock = newBlock;
        super._foundValidBlock(newBlock);
    }

    @Override
    protected MiniBlock _getNewestBlock() {
        if (_secretBlock != null) {
            final MiniBlock secretBlock = _secretBlock;
            _secretBlock = null;
            return secretBlock;
        }
        else {
            return super._getNewestBlock();
        }
    }

    @Override
    protected void _broadcastBlock(MiniBlock newMiniBlock) {
        final long now = System.currentTimeMillis();
        final long timeElapsedSinceLastBlock = (now - _timeLastBlockMined);
        _timeLastBlockMined = now;

        final long delay = (SelfishMiningSimulation.estimatedTimePerBlock - timeElapsedSinceLastBlock) * 3 / 4;
        if (delay > 0) {
            (new Thread(new Runnable() {
                @Override
                public void run() {
                    Logger.log("***** Being selfish for " + delay);
                    try { Thread.sleep(delay); } catch (final Exception e) { }
                    SelfishMiner.super._broadcastBlock(newMiniBlock);
                }
            })).start();
        }
        else {
            super._broadcastBlock(newMiniBlock);
        }
    }

    @Override
    public synchronized void onBlockMined() {
        _timeLastBlockMined = System.currentTimeMillis();

        if (_secretBlock != null) {
            Logger.log("SelfishMiner lost a block.");
        }
        _secretBlock = null;

        super.onBlockMined();
    }
}
