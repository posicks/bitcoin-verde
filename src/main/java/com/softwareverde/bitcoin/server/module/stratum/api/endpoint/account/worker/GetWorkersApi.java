package com.softwareverde.bitcoin.server.module.stratum.api.endpoint.account.worker;

import com.softwareverde.bitcoin.miner.pool.AccountId;
import com.softwareverde.bitcoin.miner.pool.WorkerId;
import com.softwareverde.bitcoin.server.Configuration;
import com.softwareverde.bitcoin.server.module.stratum.api.endpoint.StratumApiResult;
import com.softwareverde.bitcoin.server.module.stratum.database.AccountDatabaseManager;
import com.softwareverde.constable.list.List;
import com.softwareverde.database.DatabaseException;
import com.softwareverde.database.mysql.MysqlDatabaseConnection;
import com.softwareverde.database.mysql.MysqlDatabaseConnectionFactory;
import com.softwareverde.io.Logger;
import com.softwareverde.json.Json;
import com.softwareverde.servlet.AuthenticatedServlet;
import com.softwareverde.servlet.GetParameters;
import com.softwareverde.servlet.PostParameters;
import com.softwareverde.servlet.request.Request;
import com.softwareverde.servlet.response.JsonResponse;
import com.softwareverde.servlet.response.Response;

import static com.softwareverde.servlet.response.Response.ResponseCodes;

public class GetWorkersApi extends AuthenticatedServlet {
    protected final MysqlDatabaseConnectionFactory _databaseConnectionFactory;

    public GetWorkersApi(final Configuration.StratumProperties stratumProperties, final MysqlDatabaseConnectionFactory databaseConnectionFactory) {
        super(stratumProperties);
        _databaseConnectionFactory = databaseConnectionFactory;
    }

    @Override
    protected Response _onAuthenticatedRequest(final AccountId accountId, final Request request) {
        final GetParameters getParameters = request.getGetParameters();
        final PostParameters postParameters = request.getPostParameters();

        if (request.getMethod() != Request.HttpMethod.GET) {
            return new JsonResponse(ResponseCodes.BAD_REQUEST, new StratumApiResult(false, "Invalid method."));
        }

        {   // GET WORKERS
            // Requires GET:
            // Requires POST:

            try (final MysqlDatabaseConnection databaseConnection = _databaseConnectionFactory.newConnection()) {
                final AccountDatabaseManager accountDatabaseManager = new AccountDatabaseManager(databaseConnection);
                final List<WorkerId> workerIds = accountDatabaseManager.getWorkerIds(accountId);

                final Json workersJson = new Json(true);

                for (final WorkerId workerId : workerIds) {
                    final String workerUsername = accountDatabaseManager.getWorkerUsername(workerId);
                    final Long workerSharesCount = accountDatabaseManager.getWorkerSharesCount(workerId);

                    final Json workerJson = new Json(false);
                    workerJson.put("id", workerId);
                    workerJson.put("username", workerUsername);
                    workerJson.put("sharesCount", workerSharesCount);
                    workersJson.add(workerJson);
                }

                final StratumApiResult result = new StratumApiResult(true, null);
                result.put("workers", workersJson);
                return new JsonResponse(ResponseCodes.OK, result);
            }
            catch (final DatabaseException exception) {
                Logger.log(exception);
                return new JsonResponse(ResponseCodes.SERVER_ERROR, new StratumApiResult(false, "An internal error occurred."));
            }
        }
    }
}
