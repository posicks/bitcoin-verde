package com.softwareverde.bitcoin.block.validator.thread;

import com.softwareverde.bitcoin.server.module.node.database.fullnode.FullNodeDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.fullnode.FullNodeDatabaseManagerFactory;
import com.softwareverde.concurrent.pool.ThreadPool;
import com.softwareverde.constable.list.List;
import com.softwareverde.io.Logger;
import com.softwareverde.util.Container;

class ValidationTask<T, S> implements Runnable {
    protected final FullNodeDatabaseManagerFactory _databaseManagerFactory;
    protected final TaskHandler<T, S> _taskHandler;
    protected final List<T> _list;

    protected final Container<Boolean> _shouldAbort = new Container<Boolean>(false);
    protected final Container<Boolean> _isFinished = new Container<Boolean>(false);
    protected final Container<Boolean> _didEncounterError = new Container<Boolean>(false);

    protected int _startIndex;
    protected int _itemCount;

    protected void _reset() {
        _shouldAbort.value = false;
        synchronized (_isFinished) {
            _isFinished.value = false;
        }
        _didEncounterError.value = false;
    }

    public ValidationTask(final FullNodeDatabaseManagerFactory databaseManagerFactory, final List<T> list, final TaskHandler<T, S> taskHandler) {
        _databaseManagerFactory = databaseManagerFactory;
        _list = list;
        _taskHandler = taskHandler;
    }

    public void setStartIndex(final int startIndex) {
        _startIndex = startIndex;
    }

    public void setItemCount(final int itemCount) {
        _itemCount = itemCount;
    }

    public void enqueueTo(final ThreadPool threadPool) {
        threadPool.execute(this);
    }

    @Override
    public void run() {
        _reset();

        try (final FullNodeDatabaseManager databaseManager = _databaseManagerFactory.newDatabaseManager()) {
            _taskHandler.init(databaseManager);

            for (int j = 0; j < _itemCount; ++j) {
                if (_shouldAbort.value) { return; }

                final T item = _list.get(_startIndex + j);
                _taskHandler.executeTask(item);
            }
        }
        catch (final Exception exception) {
            Logger.log(exception);
            _didEncounterError.value = true;
        }
        finally {
            synchronized (_isFinished) {
                _isFinished.value = true;
                _isFinished.notifyAll();
            }
        }
    }

    public S getResult() {
        if (_didEncounterError.value) { return null; }

        synchronized (_isFinished) {
            if (! _isFinished.value) {
                try {
                    _isFinished.wait();
                }
                catch (final Exception exception) {
                    Logger.log(exception);

                    final Thread currentThread = Thread.currentThread();
                    currentThread.interrupt(); // Do not consume the interrupted status...

                    return null;
                }
            }
        }

        return _taskHandler.getResult();
    }

    public void abort() {
        _shouldAbort.value = true;
        _didEncounterError.value = true;
    }
}