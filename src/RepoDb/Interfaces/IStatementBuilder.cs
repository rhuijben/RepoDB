namespace RepoDb.Interfaces;

/// <summary>
/// An interface that is used to mark a class to be a statement builder object. The statement builder is an object that is being mapped and/or injected into the repositories to be used for
/// composing the SQL Statements. Implement this interface if the caller would likely to support the different statement building approach, or by supporting the other data providers.
/// </summary>
public interface IStatementBuilder
{
    #region CreateAverage

    /// <summary>
    /// Creates a SQL Statement for 'Average' operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="field">The field to be averaged.</param>
    /// <param name="where">The query expression.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for average operation.</returns>
    string CreateAverage(
        string tableName,
        DbFieldCollection dbFields,
        Field field,
        QueryGroup? where = null,
        string? hints = null);

    #endregion

    #region CreateAverageAll

    /// <summary>
    /// Creates a SQL Statement for 'AverageAll' operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="field">The field to be averaged.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for average-all operation.</returns>
    string CreateAverageAll(
        string tableName,
        DbFieldCollection dbFields,
        Field field,
        string? hints = null);

    #endregion

    #region CreateBatchQuery

    /// <summary>
    /// Creates a SQL Statement for 'BatchQuery' operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="page">The page of the batch.</param>
    /// <param name="rowsPerBatch">The number of rows per batch.</param>
    /// <param name="orderBy">The list of fields for ordering.</param>
    /// <param name="where">The query expression.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for batch query operation.</returns>
    string CreateBatchQuery(
        string tableName,
        DbFieldCollection dbFields,
        IEnumerable<Field> fields,
        int page,
        int rowsPerBatch,
        IEnumerable<OrderField>? orderBy = null,
        QueryGroup? where = null,
        string? hints = null);

    #endregion

    #region CreateSkipQuery

    /// <summary>
    /// Creates a SQL Statement for 'BatchQuery' operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="fields">The mapping list of <see cref="Field"/> objects to be used.</param>
    /// <param name="skip">The number of rows to skip.</param>
    /// <param name="take">The number of rows per batch.</param>
    /// <param name="orderBy">The list of fields for ordering.</param>
    /// <param name="where">The query expression.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for batch query operation.</returns>
    string CreateSkipQuery(
        string tableName,
        DbFieldCollection dbFields,
        IEnumerable<Field> fields,
        int skip,
        int take,
        IEnumerable<OrderField>? orderBy = null,
        QueryGroup? where = null,
        string? hints = null);

    #endregion

    #region CreateCount

    /// <summary>
    /// Creates a SQL Statement for 'Count' operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="where">The query expression.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for count operation.</returns>
    string CreateCount(
        string tableName,
        DbFieldCollection dbFields,
        QueryGroup? where = null,
        string? hints = null);

    #endregion

    #region CreateCountAll

    /// <summary>
    /// Creates a SQL Statement for 'CountAll' operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for count-all operation.</returns>
    string CreateCountAll(
        string tableName,
        DbFieldCollection dbFields,
        string? hints = null);

    #endregion

    #region CreateDelete

    /// <summary>
    /// Creates a SQL Statement for 'Delete' operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="where">The query expression.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for delete operation.</returns>
    string CreateDelete(
        string tableName,
        DbFieldCollection dbFields,
        QueryGroup? where = null,
        string? hints = null);

    #endregion

    #region CreateDeleteAll

    /// <summary>
    /// Creates a SQL Statement for 'DeleteAll' operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for delete-all operation.</returns>
    string CreateDeleteAll(
        string tableName,
        DbFieldCollection dbFields,
        string? hints = null);

    #endregion

    #region CreateExists

    /// <summary>
    /// Creates a SQL Statement for 'Exists' operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="where">The query expression.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for exists operation.</returns>
    string CreateExists(
        string tableName,
        DbFieldCollection dbFields,
        QueryGroup? where = null,
        string? hints = null);

    #endregion

    #region CreateInsert

    /// <summary>
    /// Creates a SQL Statement for 'Insert' operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="fields">The list of fields to be inserted.</param>
    /// <param name="primaryField">The primary field from the database.</param>
    /// <param name="identityField">The identity field from the database.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for insert operation.</returns>
    string CreateInsert(
        string tableName,
        DbFieldCollection dbFields,
        IEnumerable<Field>? fields,
        string? hints = null);

    #endregion

    #region CreateInsertAll

    /// <summary>
    /// Creates a SQL Statement for 'InsertAll' operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="fields">The list of fields to be inserted.</param>
    /// <param name="batchSize">The batch size of the operation.</param>
    /// <param name="primaryField">The primary field from the database.</param>
    /// <param name="identityField">The identity field from the database.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for insert operation.</returns>
    string CreateInsertAll(
        string tableName,
        DbFieldCollection dbFields,
        IEnumerable<Field>? fields,
        int batchSize = Constant.DefaultBatchOperationSize,
        string? hints = null);

    #endregion

    #region CreateMax

    /// <summary>
    /// Creates a SQL Statement for 'Max' operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="field">The field to be maximized.</param>
    /// <param name="where">The query expression.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for maximum operation.</returns>
    string CreateMax(
        string tableName,
        DbFieldCollection dbFields,
        Field field,
        QueryGroup? where = null,
        string? hints = null);

    #endregion

    #region CreateMaxAll

    /// <summary>
    /// Creates a SQL Statement for 'MaxAll' operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="field">The field to be maximized.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for maximum-all operation.</returns>
    string CreateMaxAll(
        string tableName,
        DbFieldCollection dbFields,
        Field field,
        string? hints = null);

    #endregion

    #region CreateMerge

    /// <summary>
    /// Creates a SQL Statement for 'Merge' operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="fields">The list of fields to be merged.</param>
    /// <param name="qualifiers">The list of the qualifier <see cref="Field"/> objects.</param>
    /// <param name="primaryField">The primary field from the database.</param>
    /// <param name="identityField">The identity field from the database.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for merge operation.</returns>
    string CreateMerge(
        string tableName,
        DbFieldCollection dbFields,
        IEnumerable<Field> fields,
        IEnumerable<Field>? qualifiers,
        string? hints = null);

    #endregion

    #region CreateMergeAll

    /// <summary>
    /// Creates a SQL Statement for 'MergeAll' operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="fields">The list of fields to be updated.</param>
    /// <param name="qualifiers">The list of the qualifier <see cref="Field"/> objects.</param>
    /// <param name="batchSize">The batch size of the operation.</param>
    /// <param name="primaryField">The primary field from the database.</param>
    /// <param name="identityField">The identity field from the database.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for update-all operation.</returns>
    string CreateMergeAll(
        string tableName,
        DbFieldCollection dbFields,
        IEnumerable<Field> fields,
        IEnumerable<Field> qualifiers,
        int batchSize = Constant.DefaultBatchOperationSize,
        string? hints = null);

    #endregion

    #region CreateMin

    /// <summary>
    /// Creates a SQL Statement for 'Min' operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="field">The field to be minimized.</param>
    /// <param name="where">The query expression.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for minimum operation.</returns>
    string CreateMin(
        string tableName,
        DbFieldCollection dbFields,
        Field field,
        QueryGroup? where = null,
        string? hints = null);

    #endregion

    #region CreateMinAll

    /// <summary>
    /// Creates a SQL Statement for 'MinAll' operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="field">The field to be minimized.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for minimum-all operation.</returns>
    string CreateMinAll(
        string tableName,
        DbFieldCollection dbFields,
        Field field,
        string? hints = null);

    #endregion

    #region CreateQuery

    /// <summary>
    /// Creates a SQL Statement for 'Query' operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="fields">The list of fields.</param>
    /// <param name="where">The query expression.</param>
    /// <param name="orderBy">The list of fields for ordering.</param>
    /// <param name="top">The number of rows to be returned.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for query operation.</returns>
    string CreateQuery(
        string tableName,
        DbFieldCollection dbFields,
        IEnumerable<Field> fields,
        QueryGroup? where = null,
        IEnumerable<OrderField>? orderBy = null,
        int? top = null,
        string? hints = null);

    #endregion

    #region CreateQueryAll

    /// <summary>
    /// Creates a SQL Statement for 'QueryAll' operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="fields">The list of fields.</param>
    /// <param name="orderBy">The list of fields for ordering.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for query operation.</returns>
    string CreateQueryAll(
        string tableName,
        DbFieldCollection dbFields,
        IEnumerable<Field> fields,
        IEnumerable<OrderField>? orderBy = null,
        string? hints = null);

    #endregion

    #region CreateSum

    /// <summary>
    /// Creates a SQL Statement for 'Sum' operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="field">The field to be summarized.</param>
    /// <param name="where">The query expression.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for sum operation.</returns>
    string CreateSum(
        string tableName,
        DbFieldCollection dbFields,
        Field field,
        QueryGroup? where = null,
        string? hints = null);

    #endregion

    #region CreateSumAll

    /// <summary>
    /// Creates a SQL Statement for 'SumAll' operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="field">The field to be summarized.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for sum-all operation.</returns>
    string CreateSumAll(
        string tableName,
        DbFieldCollection dbFields,
        Field field,
        string? hints = null);

    #endregion

    #region CreateTruncate

    /// <summary>
    /// Creates a SQL Statement for 'Truncate' operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <returns>A sql statement for truncate operation.</returns>
    string CreateTruncate(
        string tableName,
        DbFieldCollection dbFields);

    #endregion

    #region CreateUpdate

    /// <summary>
    /// Creates a SQL Statement for 'Update' operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="fields">The list of fields to be updated.</param>
    /// <param name="where">The query expression.</param>
    /// <param name="primaryField">The primary field from the database.</param>
    /// <param name="identityField">The identity field from the database.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for update operation.</returns>
    string CreateUpdate(
        string tableName,
        DbFieldCollection dbFields,
        IEnumerable<Field> fields,
        QueryGroup? where = null,
        string? hints = null);

    #endregion

    #region CreateUpdateAll

    /// <summary>
    /// Creates a SQL Statement for 'UpdateAll' operation.
    /// </summary>
    /// <param name="tableName">The name of the target table.</param>
    /// <param name="fields">The list of fields to be updated.</param>
    /// <param name="qualifiers">The list of the qualifier <see cref="Field"/> objects.</param>
    /// <param name="batchSize">The batch size of the operation.</param>
    /// <param name="primaryField">The primary field from the database.</param>
    /// <param name="identityField">The identity field from the database.</param>
    /// <param name="hints">The table hints to be used.</param>
    /// <returns>A sql statement for update-all operation.</returns>
    string CreateUpdateAll(
        string tableName,
        DbFieldCollection dbFields,
        IEnumerable<Field> fields,
        IEnumerable<Field> qualifiers,
        int batchSize = Constant.DefaultBatchOperationSize,
        string? hints = null);

    #endregion
}
