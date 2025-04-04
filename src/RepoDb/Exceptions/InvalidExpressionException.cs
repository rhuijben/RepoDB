namespace RepoDb.Exceptions;

/// <summary>
/// An exception that is being thrown when the query expression passed is not valid.
/// </summary>
public class InvalidExpressionException : Exception
{
    /// <summary>
    /// Creates a new instance of <see cref="InvalidExpressionException"/> class.
    /// </summary>
    /// <param name="message">The exception message.</param>
    public InvalidExpressionException(string message)
        : base(message) { }
    public InvalidExpressionException()
    {
    }
    public InvalidExpressionException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}
