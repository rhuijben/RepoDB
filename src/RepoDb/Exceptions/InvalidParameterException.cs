namespace RepoDb.Exceptions;

/// <summary>
/// An exception that is being thrown when the parameter is not valid.
/// </summary>
public class InvalidParameterException : ArgumentException
{
    /// <summary>
    /// Creates a new instance of <see cref="InvalidParameterException"/> class.
    /// </summary>
    /// <param name="message">The exception message.</param>
    public InvalidParameterException(string? message)
        : base(message: message, innerException: null) { }
    public InvalidParameterException()
    {
    }
    public InvalidParameterException(string message, Exception innerException) : base(message, innerException)
    {
    }
}
