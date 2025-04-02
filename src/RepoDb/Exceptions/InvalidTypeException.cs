namespace RepoDb.Exceptions;

/// <summary>
/// An exception that is being thrown when the type is not valid.
/// </summary>
public class InvalidTypeException : ArgumentOutOfRangeException
{
    /// <summary>
    /// Creates a new instance of <see cref="InvalidTypeException"/> class.
    /// </summary>
    /// <param name="message">The exception message.</param>
    public InvalidTypeException(string? message)
        : base(message: message, innerException: null) { }
    public InvalidTypeException()
    {
    }
    public InvalidTypeException(string message, Exception innerException) : base(message, innerException)
    {
    }
}
