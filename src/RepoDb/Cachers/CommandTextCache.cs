using System.Collections.Concurrent;
using System.Data;
using RepoDb.Exceptions;
using RepoDb.Extensions;
using RepoDb.Interfaces;
using RepoDb.Requests;

namespace RepoDb;

/// <summary>
/// A class that is being used to cache the already-built command texts.
/// </summary>
public static class CommandTextCache
{
    private static readonly ConcurrentDictionary<BaseRequest, string> cache = new();

    #region GetAverageText

    /// <summary>
    ///
    /// </summary>
    /// <param name="request"></param>
    /// <returns></returns>
    internal static string GetAverageText(AverageRequest request)
    {
        if (cache.TryGetValue(request, out var commandText) == false)
        {
            var dbFields = DbFieldCache.Get(request.Connection, request.Name, request.Transaction);
            var statementBuilder = EnsureStatementBuilder(request.Connection, request.StatementBuilder);
            commandText = statementBuilder.CreateAverage(request.Name,
                dbFields,
                request.Field,
                request.Where,
                request.Hints);
            cache.TryAdd(request, commandText);
        }
        return commandText;
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="request"></param>
    /// <returns></returns>
    internal static async ValueTask<string> GetAverageTextAsync(AverageRequest request, CancellationToken cancellationToken)
    {
        if (cache.TryGetValue(request, out var commandText) == false)
        {
            var dbFields = await DbFieldCache.GetAsync(request.Connection, request.Name, request.Transaction, cancellationToken);
            var statementBuilder = EnsureStatementBuilder(request.Connection, request.StatementBuilder);
            commandText = statementBuilder.CreateAverage(request.Name,
                dbFields,
                request.Field,
                request.Where,
                request.Hints);
            cache.TryAdd(request, commandText);
        }
        return commandText;
    }

    #endregion

    #region GetAverageAllText

    /// <summary>
    ///
    /// </summary>
    /// <param name="request"></param>
    /// <returns></returns>
    internal static string GetAverageAllText(AverageAllRequest request)
    {
        if (cache.TryGetValue(request, out var commandText) == false)
        {
            var dbFields = DbFieldCache.Get(request.Connection, request.Name, request.Transaction);
            var statementBuilder = EnsureStatementBuilder(request.Connection, request.StatementBuilder);
            commandText = statementBuilder.CreateAverageAll(request.Name,
                dbFields,
                request.Field,
                request.Hints);
            cache.TryAdd(request, commandText);
        }
        return commandText;
    }

    internal static async ValueTask<string> GetAverageAllTextAsync(AverageAllRequest request, CancellationToken cancellationToken)
    {
        if (cache.TryGetValue(request, out var commandText) == false)
        {
            var dbFields = await DbFieldCache.GetAsync(request.Connection, request.Name, request.Transaction, cancellationToken);
            var statementBuilder = EnsureStatementBuilder(request.Connection, request.StatementBuilder);
            commandText = statementBuilder.CreateAverageAll(request.Name,
                dbFields,
                request.Field,
                request.Hints);
            cache.TryAdd(request, commandText);
        }
        return commandText;
    }

    #endregion

    #region GetBatchQueryText

    /// <summary>
    ///
    /// </summary>
    /// <param name="request"></param>
    /// <returns></returns>
    internal static string GetBatchQueryText(BatchQueryRequest request)
    {
        if (cache.TryGetValue(request, out var commandText) == false)
        {
            var dbSetting = request.Connection.GetDbSetting();
            var dbFields = DbFieldCache.Get(request.Connection, request.Name, request.Transaction);
            var fields = GetTargetFields(dbFields, request.Fields, dbSetting);
            ValidateOrderFields(dbFields, request.OrderBy, dbSetting);
            commandText = GetBatchQueryTextInternal(request, fields);
            cache.TryAdd(request, commandText);
        }
        return commandText;
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="request"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    internal static async ValueTask<string> GetBatchQueryTextAsync(BatchQueryRequest request,
        CancellationToken cancellationToken = default)
    {
        if (cache.TryGetValue(request, out var commandText) == false)
        {
            var dbSetting = request.Connection.GetDbSetting();
            var dbFields = await DbFieldCache.GetAsync(request.Connection, request.Name, request.Transaction, cancellationToken).ConfigureAwait(false);

            var fields = GetTargetFields(dbFields, request.Fields, dbSetting);
            ValidateOrderFields(dbFields, request.OrderBy, dbSetting);
            commandText = GetBatchQueryTextInternal(request, fields, dbFields);
            cache.TryAdd(request, commandText);
        }
        return commandText;
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="request"></param>
    /// <param name="fields"></param>
    /// <returns></returns>
    internal static string GetBatchQueryTextInternal(BatchQueryRequest request,
        IEnumerable<Field> fields,
        DbFieldCollection dbFields)
    {
        var statementBuilder = EnsureStatementBuilder(request.Connection, request.StatementBuilder);
        return statementBuilder.CreateBatchQuery(request.Name,
            dbFields,
            fields,
            request.Page,
            request.RowsPerBatch,
            request.OrderBy,
            request.Where,
            request.Hints);
    }

    #endregion

    #region GetCountText

    /// <summary>
    ///
    /// </summary>
    /// <param name="request"></param>
    /// <returns></returns>
    internal static string GetCountText(CountRequest request)
    {
        if (cache.TryGetValue(request, out var commandText) == false)
        {
            var dbFields = DbFieldCache.Get(request.Connection, request.Name, request.Transaction);
            var statementBuilder = EnsureStatementBuilder(request.Connection, request.StatementBuilder);
            commandText = statementBuilder.CreateCount(
                request.Name,
                dbFields,
                request.Where,
                request.Hints);
            cache.TryAdd(request, commandText);
        }
        return commandText;
    }

    internal static async ValueTask<string> GetCountTextAsync(CountRequest request, CancellationToken cancellationToken)
    {
        if (cache.TryGetValue(request, out var commandText) == false)
        {
            var dbFields = await DbFieldCache.GetAsync(request.Connection, request.Name, request.Transaction, cancellationToken).ConfigureAwait(false);
            var statementBuilder = EnsureStatementBuilder(request.Connection, request.StatementBuilder);
            commandText = statementBuilder.CreateCount(
                request.Name,
                dbFields,
                request.Where,
                request.Hints);
            cache.TryAdd(request, commandText);
        }
        return commandText;
    }

    #endregion

    #region GetCountAllText

    /// <summary>
    ///
    /// </summary>
    /// <param name="request"></param>
    /// <returns></returns>
    internal static string GetCountAllText(CountAllRequest request)
    {
        if (cache.TryGetValue(request, out var commandText) == false)
        {
            var statementBuilder = EnsureStatementBuilder(request.Connection, request.StatementBuilder);
            commandText = statementBuilder.CreateCountAll(request.Name,
                request.Hints);
            cache.TryAdd(request, commandText);
        }
        return commandText;
    }

    #endregion

    #region GetDeleteText

    /// <summary>
    ///
    /// </summary>
    /// <param name="request"></param>
    /// <returns></returns>
    internal static string GetDeleteText(DeleteRequest request)
    {
        if (cache.TryGetValue(request, out var commandText) == false)
        {
            var dbFields = DbFieldCache.Get(request.Connection, request.Name, request.Transaction);
            var statementBuilder = EnsureStatementBuilder(request.Connection, request.StatementBuilder);
            commandText = statementBuilder.CreateDelete(request.Name,
                dbFields,
                request.Where,
                request.Hints);
            cache.TryAdd(request, commandText);
        }
        return commandText;
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="request"></param>
    /// <returns></returns>
    internal static async ValueTask<string> GetDeleteTextAsync(DeleteRequest request, CancellationToken cancellationToken)
    {
        if (cache.TryGetValue(request, out var commandText) == false)
        {
            var dbFields = await DbFieldCache.GetAsync(request.Connection, request.Name, request.Transaction, cancellationToken).ConfigureAwait(false);
            var statementBuilder = EnsureStatementBuilder(request.Connection, request.StatementBuilder);
            commandText = statementBuilder.CreateDelete(request.Name,
                dbFields,
                request.Where,
                request.Hints);
            cache.TryAdd(request, commandText);
        }
        return commandText;
    }

    #endregion

    #region GetDeleteAllText

    /// <summary>
    ///
    /// </summary>
    /// <param name="request"></param>
    /// <returns></returns>
    internal static string GetDeleteAllText(DeleteAllRequest request)
    {
        if (cache.TryGetValue(request, out var commandText) == false)
        {
            var statementBuilder = EnsureStatementBuilder(request.Connection, request.StatementBuilder);
            commandText = statementBuilder.CreateDeleteAll(request.Name,
                request.Hints);
            cache.TryAdd(request, commandText);
        }
        return commandText;
    }

    #endregion

    #region GetExistsText

    /// <summary>
    ///
    /// </summary>
    /// <param name="request"></param>
    /// <returns></returns>
    internal static string GetExistsText(ExistsRequest request)
    {
        if (cache.TryGetValue(request, out var commandText) == false)
        {
            var statementBuilder = EnsureStatementBuilder(request.Connection, request.StatementBuilder);
            commandText = statementBuilder.CreateExists(request.Name,
                request.Where,
                request.Hints);
            cache.TryAdd(request, commandText);
        }
        return commandText;
    }

    #endregion

    #region GetInsertText

    /// <summary>
    ///
    /// </summary>
    /// <param name="request"></param>
    /// <returns></returns>
    internal static string GetInsertText(InsertRequest request)
    {
        if (cache.TryGetValue(request, out var commandText) == false)
        {
            var dbSetting = request.Connection.GetDbSetting();
            var dbFields = DbFieldCache.Get(request.Connection, request.Name, request.Transaction);
            var fields = GetTargetFields(dbFields, request.Fields, dbSetting);
            commandText = GetInsertTextInternal(request, fields, dbFields);
            cache.TryAdd(request, commandText);
        }
        return commandText;
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="request"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    internal static async ValueTask<string> GetInsertTextAsync(InsertRequest request,
        CancellationToken cancellationToken = default)
    {
        if (cache.TryGetValue(request, out var commandText) == false)
        {
            var dbSetting = request.Connection.GetDbSetting();
            var dbFields = await DbFieldCache.GetAsync(request.Connection, request.Name, request.Transaction, cancellationToken).ConfigureAwait(false);

            var fields = GetTargetFields(dbFields, request.Fields, dbSetting);
            commandText = GetInsertTextInternal(request, fields, dbFields);
            cache.TryAdd(request, commandText);
        }
        return commandText;
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="request"></param>
    /// <param name="fields"></param>
    /// <param name="primaryField"></param>
    /// <param name="identityField"></param>
    /// <returns></returns>
    private static string GetInsertTextInternal(InsertRequest request,
        IEnumerable<Field> fields,
        DbFieldCollection dbFields)
    {
        var statementBuilder = EnsureStatementBuilder(request.Connection, request.StatementBuilder);
        return statementBuilder.CreateInsert(request.Name,
            fields,
            dbFields,
            request.Hints);
    }

    #endregion

    #region GetInsertAllText

    /// <summary>
    ///
    /// </summary>
    /// <param name="request"></param>
    /// <returns></returns>
    internal static string GetInsertAllText(InsertAllRequest request)
    {
        if (cache.TryGetValue(request, out var commandText) == false)
        {
            var dbSetting = request.Connection.GetDbSetting();
            var dbFields = DbFieldCache.Get(request.Connection, request.Name, request.Transaction);
            var fields = GetTargetFields(dbFields, request.Fields, dbSetting);
            commandText = GetInsertAllTextInternal(request, fields, dbFields);
            cache.TryAdd(request, commandText);
        }
        return commandText;
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="request"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    internal static async ValueTask<string> GetInsertAllTextAsync(InsertAllRequest request,
        CancellationToken cancellationToken = default)
    {
        if (cache.TryGetValue(request, out var commandText) == false)
        {
            var dbSetting = request.Connection.GetDbSetting();
            var dbFields = DbFieldCache.GetAsync(request.Connection, request.Name, request.Transaction, cancellationToken);

            var fields = GetTargetFields(dbFields, request.Fields, dbSetting);
            commandText = GetInsertAllTextInternal(request, fields, dbFields);
            cache.TryAdd(request, commandText);
        }
        return commandText;
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="request"></param>
    /// <param name="fields"></param>
    /// <param name="primaryField"></param>
    /// <param name="identityField"></param>
    /// <returns></returns>
    private static string GetInsertAllTextInternal(InsertAllRequest request,
        IEnumerable<Field> fields,
        DbFieldCollection? dbFields = null)
    {
        var statementBuilder = EnsureStatementBuilder(request.Connection, request.StatementBuilder);
        return statementBuilder.CreateInsertAll(request.Name,
            fields,
            request.BatchSize,
            dbFields,
            request.Hints);
    }

    #endregion

    #region GetMaxText

    /// <summary>
    ///
    /// </summary>
    /// <param name="request"></param>
    /// <returns></returns>
    internal static string GetMaxText(MaxRequest request)
    {
        if (cache.TryGetValue(request, out var commandText) == false)
        {
            var statementBuilder = EnsureStatementBuilder(request.Connection, request.StatementBuilder);
            commandText = statementBuilder.CreateMax(request.Name,
                request.Field,
                request.Where,
                request.Hints);
            cache.TryAdd(request, commandText);
        }
        return commandText;
    }

    #endregion

    #region GetMaxAllText

    /// <summary>
    ///
    /// </summary>
    /// <param name="request"></param>
    /// <returns></returns>
    internal static string GetMaxAllText(MaxAllRequest request)
    {
        if (cache.TryGetValue(request, out var commandText) == false)
        {
            var statementBuilder = EnsureStatementBuilder(request.Connection, request.StatementBuilder);
            commandText = statementBuilder.CreateMaxAll(request.Name,
                request.Field,
                request.Hints);
            cache.TryAdd(request, commandText);
        }
        return commandText;
    }

    #endregion

    #region GetMergeText

    /// <summary>
    ///
    /// </summary>
    /// <param name="request"></param>
    /// <returns></returns>
    internal static string GetMergeText(MergeRequest request)
    {
        if (cache.TryGetValue(request, out var commandText) == false)
        {
            var dbSetting = request.Connection.GetDbSetting();
            var dbFields = DbFieldCache.Get(request.Connection, request.Name, request.Transaction);

            var fields = GetTargetFields(dbFields, request.Fields, dbSetting);
            commandText = GetMergeTextInternal(request, fields, dbFields);
            cache.TryAdd(request, commandText);
        }
        return commandText;
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="request"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    internal static async ValueTask<string> GetMergeTextAsync(MergeRequest request,
        CancellationToken cancellationToken = default)
    {
        if (cache.TryGetValue(request, out var commandText) == false)
        {
            var dbSetting = request.Connection.GetDbSetting();
            var dbFields = await DbFieldCache.GetAsync(request.Connection, request.Name, request.Transaction, cancellationToken);
            var fields = GetTargetFields(dbFields, request.Fields, dbSetting);
            commandText = GetMergeTextInternal(request, fields, dbFields);
            cache.TryAdd(request, commandText);
        }
        return commandText;
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="request"></param>
    /// <param name="fields"></param>
    /// <param name="primaryField"></param>
    /// <param name="identityField"></param>
    /// <returns></returns>
    private static string GetMergeTextInternal(MergeRequest request,
        IEnumerable<Field> fields,
        DbFieldCollection dbFields)
    {
        var statementBuilder = EnsureStatementBuilder(request.Connection, request.StatementBuilder);
        return statementBuilder.CreateMerge(request.Name,
            fields,
            request.Qualifiers,
            dbFields,
            request.Hints);
    }

    #endregion

    #region GetMergeAllText

    /// <summary>
    /// Gets the cached command text for the 'MergeAll' operation.
    /// </summary>
    /// <param name="request">The request object.</param>
    /// <returns>The cached command text.</returns>
    internal static string GetMergeAllText(MergeAllRequest request)
    {
        if (cache.TryGetValue(request, out var commandText) == false)
        {
            var dbSetting = request.Connection.GetDbSetting();
            var dbFields = DbFieldCache.Get(request.Connection, request.Name, request.Transaction);

            var fields = GetTargetFields(dbFields, request.Fields, dbSetting);
            commandText = GetMergeAllTextInternal(request, fields, dbFields);
            cache.TryAdd(request, commandText);
        }
        return commandText;
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="request"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    internal static async ValueTask<string> GetMergeAllTextAsync(MergeAllRequest request,
        CancellationToken cancellationToken = default)
    {
        if (cache.TryGetValue(request, out var commandText) == false)
        {
            var dbSetting = request.Connection.GetDbSetting();
            var dbFields = await DbFieldCache.GetAsync(request.Connection, request.Name, request.Transaction, cancellationToken).ConfigureAwait(false);

            var fields = GetTargetFields(dbFields, request.Fields, dbSetting);
            commandText = GetMergeAllTextInternal(request, fields, dbFields);
            cache.TryAdd(request, commandText);
        }
        return commandText;
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="request"></param>
    /// <param name="fields"></param>
    /// <param name="primaryField"></param>
    /// <param name="identityField"></param>
    /// <returns></returns>
    private static string GetMergeAllTextInternal(MergeAllRequest request,
        IEnumerable<Field> fields,
        DbFieldCollection dbFields)
    {
        var statementBuilder = EnsureStatementBuilder(request.Connection, request.StatementBuilder);
        return statementBuilder.CreateMergeAll(request.Name,
            fields,
            request.Qualifiers,
            request.BatchSize,
            dbFields,
            request.Hints);
    }

    #endregion

    #region GetMinText

    /// <summary>
    ///
    /// </summary>
    /// <param name="request"></param>
    /// <returns></returns>
    internal static string GetMinText(MinRequest request)
    {
        if (cache.TryGetValue(request, out var commandText) == false)
        {
            var statementBuilder = EnsureStatementBuilder(request.Connection, request.StatementBuilder);
            commandText = statementBuilder.CreateMin(request.Name,
                request.Field,
                request.Where,
                request.Hints);
            cache.TryAdd(request, commandText);
        }
        return commandText;
    }

    #endregion

    #region GetMinAllText

    /// <summary>
    ///
    /// </summary>
    /// <param name="request"></param>
    /// <returns></returns>
    internal static string GetMinAllText(MinAllRequest request)
    {
        if (cache.TryGetValue(request, out var commandText) == false)
        {
            var statementBuilder = EnsureStatementBuilder(request.Connection, request.StatementBuilder);
            commandText = statementBuilder.CreateMinAll(request.Name,
                request.Field,
                request.Hints);
            cache.TryAdd(request, commandText);
        }
        return commandText;
    }

    #endregion

    #region GetQueryText

    /// <summary>
    ///
    /// </summary>
    /// <param name="request"></param>
    /// <returns></returns>
    internal static string GetQueryText(QueryRequest request)
    {
        if (cache.TryGetValue(request, out var commandText) == false)
        {
            var dbSetting = request.Connection.GetDbSetting();
            var dbFields = DbFieldCache.Get(request.Connection, request.Name, request.Transaction);

            var fields = GetTargetFields(dbFields, request.Fields, dbSetting);
            ValidateOrderFields(dbFields, request.OrderBy, dbSetting);
            commandText = GetQueryTextInternal(request, fields);
            cache.TryAdd(request, commandText);
        }
        return commandText;
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="request"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    internal static async ValueTask<string> GetQueryTextAsync(QueryRequest request,
        CancellationToken cancellationToken = default)
    {
        if (cache.TryGetValue(request, out var commandText) == false)
        {
            var dbSetting = request.Connection.GetDbSetting();
            var dbFields = await DbFieldCache.GetAsync(request.Connection, request.Name, request.Transaction, cancellationToken).ConfigureAwait(false);

            var fields = GetTargetFields(dbFields, request.Fields, dbSetting);
            ValidateOrderFields(dbFields, request.OrderBy, dbSetting);
            commandText = GetQueryTextInternal(request, fields);
            cache.TryAdd(request, commandText);
        }
        return commandText;
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="request"></param>
    /// <param name="fields"></param>
    /// <returns></returns>
    private static string GetQueryTextInternal(QueryRequest request,
        IEnumerable<Field> fields)
    {
        var statementBuilder = EnsureStatementBuilder(request.Connection, request.StatementBuilder);
        return statementBuilder.CreateQuery(request.Name,
            fields,
            request.Where,
            request.OrderBy,
            request.Top,
            request.Hints);
    }

    #endregion

    #region GetQueryAllText

    /// <summary>
    ///
    /// </summary>
    /// <param name="request"></param>
    /// <returns></returns>
    internal static string GetQueryAllText(QueryAllRequest request)
    {
        if (cache.TryGetValue(request, out var commandText) == false)
        {
            var dbSetting = request.Connection.GetDbSetting();
            var dbFields = DbFieldCache.Get(request.Connection, request.Name, request.Transaction);

            var fields = GetTargetFields(dbFields, request.Fields, dbSetting);
            ValidateOrderFields(dbFields, request.OrderBy, dbSetting);
            commandText = GetQueryAllTextInternal(request, fields);
            cache.TryAdd(request, commandText);
        }
        return commandText;
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="request"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    internal static async ValueTask<string> GetQueryAllTextAsync(QueryAllRequest request,
        CancellationToken cancellationToken = default)
    {
        if (cache.TryGetValue(request, out var commandText) == false)
        {
            var dbSetting = request.Connection.GetDbSetting();
            var dbFields = await DbFieldCache.GetAsync(request.Connection, request.Name, request.Transaction, cancellationToken).ConfigureAwait(false);

            var fields = GetTargetFields(dbFields, request.Fields, dbSetting);
            ValidateOrderFields(dbFields, request.OrderBy, dbSetting);
            commandText = GetQueryAllTextInternal(request, fields);
            cache.TryAdd(request, commandText);
        }
        return commandText;
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="request"></param>
    /// <param name="fields"></param>
    /// <returns></returns>
    private static string GetQueryAllTextInternal(QueryAllRequest request,
        IEnumerable<Field> fields)
    {
        var statementBuilder = EnsureStatementBuilder(request.Connection, request.StatementBuilder);
        return statementBuilder.CreateQueryAll(request.Name,
            fields,
            request.OrderBy,
            request.Hints);
    }

    #endregion

    #region GetQueryMultipleText

    /// <summary>
    ///
    /// </summary>
    /// <param name="request"></param>
    /// <returns></returns>
    internal static string GetQueryMultipleText(QueryMultipleRequest request)
    {
        if (cache.TryGetValue(request, out var commandText) == false)
        {
            var dbSetting = request.Connection.GetDbSetting();
            var dbFields = DbFieldCache.Get(request.Connection, request.Name, request.Transaction);

            var fields = GetTargetFields(dbFields, request.Fields, dbSetting);
            ValidateOrderFields(dbFields, request.OrderBy, dbSetting);
            commandText = GetQueryMultipleTextInternal(request, fields);
            cache.TryAdd(request, commandText);
        }
        return commandText;
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="request"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    internal static async ValueTask<string> GetQueryMultipleTextAsync(QueryMultipleRequest request,
        CancellationToken cancellationToken = default)
    {
        if (cache.TryGetValue(request, out var commandText) == false)
        {
            var dbSetting = request.Connection.GetDbSetting();
            var dbFields = await DbFieldCache.GetAsync(request.Connection, request.Name, request.Transaction, cancellationToken).ConfigureAwait(false);

            var fields = GetTargetFields(dbFields, request.Fields, dbSetting);
            ValidateOrderFields(dbFields, request.OrderBy, dbSetting);
            commandText = GetQueryMultipleTextInternal(request, fields);
            cache.TryAdd(request, commandText);
        }
        return commandText;
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="request"></param>
    /// <param name="fields"></param>
    /// <returns></returns>
    private static string GetQueryMultipleTextInternal(QueryMultipleRequest request,
        IEnumerable<Field> fields)
    {
        var statementBuilder = EnsureStatementBuilder(request.Connection, request.StatementBuilder);
        return statementBuilder.CreateQuery(request.Name,
            fields,
            request.Where,
            request.OrderBy,
            request.Top,
            request.Hints);
    }

    #endregion

    #region GetSkipQueryText

    /// <summary>
    ///
    /// </summary>
    /// <param name="request"></param>
    /// <returns></returns>
    internal static string GetSkipQueryText(SkipQueryRequest request)
    {
        if (cache.TryGetValue(request, out var commandText) == false)
        {
            var dbSetting = request.Connection.GetDbSetting();
            var dbFields = DbFieldCache.Get(request.Connection, request.Name, request.Transaction);

            var fields = GetTargetFields(dbFields, request.Fields, dbSetting);
            ValidateOrderFields(dbFields, request.OrderBy, dbSetting);

            commandText = GetSkipQueryTextInternal(request, fields);
            cache.TryAdd(request, commandText);
        }
        return commandText;
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="request"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    internal static async ValueTask<string> GetSkipQueryTextAsync(SkipQueryRequest request,
        CancellationToken cancellationToken = default)
    {
        if (cache.TryGetValue(request, out var commandText) == false)
        {
            var dbSetting = request.Connection.GetDbSetting();
            var dbFields = await DbFieldCache.GetAsync(request.Connection, request.Name, request.Transaction, cancellationToken).ConfigureAwait(false);

            var fields = GetTargetFields(dbFields, request.Fields, dbSetting);
            ValidateOrderFields(dbFields, request.OrderBy, dbSetting);
            commandText = GetSkipQueryTextInternal(request, fields);
            cache.TryAdd(request, commandText);
        }
        return commandText;
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="request"></param>
    /// <param name="fields"></param>
    /// <returns></returns>
    internal static string GetSkipQueryTextInternal(SkipQueryRequest request,
        IEnumerable<Field> fields)
    {
        var statementBuilder = EnsureStatementBuilder(request.Connection, request.StatementBuilder);
        return statementBuilder.CreateSkipQuery(request.Name,
            fields,
            request.Skip,
            request.RowsPerBatch,
            request.OrderBy,
            request.Where,
            request.Hints);
    }

    #endregion


    #region GetSumText

    /// <summary>
    ///
    /// </summary>
    /// <param name="request"></param>
    /// <returns></returns>
    internal static string GetSumText(SumRequest request)
    {
        if (cache.TryGetValue(request, out var commandText) == false)
        {
            var statementBuilder = EnsureStatementBuilder(request.Connection, request.StatementBuilder);
            commandText = statementBuilder.CreateSum(request.Name,
                request.Field,
                request.Where,
                request.Hints);
            cache.TryAdd(request, commandText);
        }
        return commandText;
    }

    #endregion

    #region GetSumAllText

    /// <summary>
    ///
    /// </summary>
    /// <param name="request"></param>
    /// <returns></returns>
    internal static string GetSumAllText(SumAllRequest request)
    {
        if (cache.TryGetValue(request, out var commandText) == false)
        {
            var statementBuilder = EnsureStatementBuilder(request.Connection, request.StatementBuilder);
            commandText = statementBuilder.CreateSumAll(request.Name,
                request.Field,
                request.Hints);
            cache.TryAdd(request, commandText);
        }
        return commandText;
    }

    #endregion

    #region GetTruncateText

    /// <summary>
    ///
    /// </summary>
    /// <param name="request"></param>
    /// <returns></returns>
    internal static string GetTruncateText(TruncateRequest request)
    {
        if (cache.TryGetValue(request, out var commandText) == false)
        {
            var statementBuilder = EnsureStatementBuilder(request.Connection, request.StatementBuilder);
            commandText = statementBuilder.CreateTruncate(request.Name, dbFields: default!);
            cache.TryAdd(request, commandText);
        }
        return commandText;
    }

    #endregion

    #region GetUpdateText

    /// <summary>
    ///
    /// </summary>
    /// <param name="request"></param>
    /// <returns></returns>
    internal static string GetUpdateText(UpdateRequest request)
    {
        if (cache.TryGetValue(request, out var commandText) == false)
        {
            var dbSetting = request.Connection.GetDbSetting();
            var dbFields = DbFieldCache.Get(request.Connection, request.Name, request.Transaction);

            var fields = GetTargetFields(dbFields, request.Fields, dbSetting);
            commandText = GetUpdateTextInternal(request, fields, dbFields);
            cache.TryAdd(request, commandText);
        }
        return commandText;
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="request"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    internal static async ValueTask<string> GetUpdateTextAsync(UpdateRequest request,
        CancellationToken cancellationToken = default)
    {
        if (cache.TryGetValue(request, out var commandText) == false)
        {
            var dbSetting = request.Connection.GetDbSetting();
            var dbFields = await DbFieldCache.GetAsync(request.Connection, request.Name, request.Transaction, cancellationToken).ConfigureAwait(false);

            var fields = GetTargetFields(dbFields, request.Fields, dbSetting);
            commandText = GetUpdateTextInternal(request, fields, dbFields);
            cache.TryAdd(request, commandText);
        }
        return commandText;
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="request"></param>
    /// <param name="fields"></param>
    /// <param name="primaryField"></param>
    /// <param name="identityField"></param>
    /// <returns></returns>
    private static string GetUpdateTextInternal(UpdateRequest request,
        IEnumerable<Field> fields,
        DbFieldCollection dbFields)
    {
        var statementBuilder = EnsureStatementBuilder(request.Connection, request.StatementBuilder);
        return statementBuilder.CreateUpdate(request.Name,
            fields,
            request.Where,
            dbFields,
            request.Hints);
    }

    #endregion

    #region GetUpdateAllText

    /// <summary>
    ///
    /// </summary>
    /// <param name="request"></param>
    /// <returns></returns>
    internal static string GetUpdateAllText(UpdateAllRequest request)
    {
        if (cache.TryGetValue(request, out var commandText) == false)
        {
            var dbSetting = request.Connection.GetDbSetting();
            var dbFields = DbFieldCache.Get(request.Connection, request.Name, request.Transaction);

            var fields = GetTargetFields(dbFields, request.Fields, dbSetting);
            commandText = GetUpdateAllTextInternal(request, fields, dbFields);
            cache.TryAdd(request, commandText);
        }
        return commandText;
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="request"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    internal static async ValueTask<string> GetUpdateAllTextAsync(UpdateAllRequest request,
        CancellationToken cancellationToken = default)
    {
        if (cache.TryGetValue(request, out var commandText) == false)
        {
            var dbSetting = request.Connection.GetDbSetting();
            var dbFields = await DbFieldCache.GetAsync(request.Connection, request.Name, request.Transaction, cancellationToken).ConfigureAwait(false);

            var fields = GetTargetFields(dbFields, request.Fields, dbSetting);
            commandText = GetUpdateAllTextInternal(request, fields, dbFields);
            cache.TryAdd(request, commandText);
        }
        return commandText;
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="request"></param>
    /// <param name="fields"></param>
    /// <param name="primaryField"></param>
    /// <param name="identityField"></param>
    /// <returns></returns>
    private static string GetUpdateAllTextInternal(UpdateAllRequest request,
        IEnumerable<Field> fields,
        DbFieldCollection dbFields)
    {
        var statementBuilder = EnsureStatementBuilder(request.Connection, request.StatementBuilder);
        return statementBuilder.CreateUpdateAll(request.Name,
            fields,
            request.Qualifiers,
            request.BatchSize,
            dbFields,
            request.Hints);
    }

    #endregion

    #region Helper Methods

    /// <summary>
    /// Flushes all the existing cached command texts.
    /// </summary>
    public static void Flush() =>
        cache.Clear();

    /// <summary>
    ///
    /// </summary>
    /// <param name="connection"></param>
    /// <param name="tableName"></param>
    /// <param name="orderFields"></param>
    /// <param name="transaction"></param>
    private static void ValidateOrderFields(DbFieldCollection dbFields,
        IEnumerable<OrderField> orderFields,
        IDbSetting dbSetting)
    {
        if (orderFields?.Any() != true)
            return;

        var unmatchesOrderFields = dbFields?.IsEmpty() == false ?
            orderFields
                .Where(of =>
                    dbFields.GetByUnquotedName(of.Name.AsUnquoted(true, dbSetting)) == null) : null;
        if (unmatchesOrderFields?.Any() == true)
        {
            throw new MissingFieldsException($"The order fields '{unmatchesOrderFields.Select(of => of.Name).Join(", ")}' are not present from the actual table.");
        }
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="connection"></param>
    /// <param name="tableName"></param>
    /// <param name="fields"></param>
    /// <param name="transaction"></param>
    /// <returns></returns>
    private static IEnumerable<Field> GetTargetFields(DbFieldCollection dbFields,
        IEnumerable<Field> fields,
        IDbSetting dbSetting)
    {
        if (fields?.Any() != true)
        {
            return null;
        }
        return GetTargetFieldsInternal(fields, dbFields, dbSetting);
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="fields"></param>
    /// <param name="dbFields"></param>
    /// <param name="dbSetting"></param>
    /// <returns></returns>
    private static IEnumerable<Field> GetTargetFieldsInternal(IEnumerable<Field> fields,
        DbFieldCollection? dbFields,
        IDbSetting dbSetting)
    {
        return dbFields?.Count > 0 ?
            fields
                .Where(f =>
                    dbFields.GetByUnquotedName(f.Name.AsUnquoted(true, dbSetting)) is { } dbf && !dbf.IsReadOnly) :
            fields;
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="request"></param>
    /// <returns></returns>
    private static IEnumerable<DbField>? GetPrimaryFields(BaseRequest request)
    {
        var dbFields = DbFieldCache.Get(request.Connection, request.Name, request.Transaction);
        return dbFields.GetPrimaryFields();
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="request"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    private static async ValueTask<IEnumerable<DbField>?> GetPrimaryFieldsAsync(BaseRequest request,
        CancellationToken cancellationToken = default)
    {
        var dbFields = await DbFieldCache.GetAsync(request.Connection, request.Name, request.Transaction, cancellationToken).ConfigureAwait(false);
        return dbFields.GetPrimaryFields();
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="request"></param>
    /// <returns></returns>
    private static DbField? GetIdentityField(BaseRequest request)
    {
        var dbFields = DbFieldCache.Get(request.Connection, request.Name, request.Transaction);
        return dbFields.GetIdentity();
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="request"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    private static async ValueTask<DbField> GetIdentityFieldAsync(BaseRequest request,
        CancellationToken cancellationToken = default)
    {
        var dbFields = await DbFieldCache.GetAsync(request.Connection, request.Name, request.Transaction, cancellationToken).ConfigureAwait(false);
        return dbFields?.GetIdentity();
    }

    /// <summary>
    ///
    /// </summary>
    /// <param name="connection"></param>
    /// <param name="builder"></param>
    /// <returns></returns>
    private static IStatementBuilder EnsureStatementBuilder(IDbConnection connection,
        IStatementBuilder builder) =>
        builder ?? connection.GetStatementBuilder();

    #endregion
}
