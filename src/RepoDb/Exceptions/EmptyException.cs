namespace RepoDb.Exceptions;

/// <summary>
/// An exception that is being thrown if the <see cref="Array"/> or <see cref="Enumerable"/> is empty.
/// </summary>
public class EmptyException : Exception
{
    /// <summary>
    /// Creates a new instance of <see cref="EmptyException"/> class.
    /// </summary>
    /// <param name="message">The exception message.</param>
    public EmptyException(string message) : base(message) { }
    public EmptyException()
    {
    }
    public EmptyException(string message, Exception innerException) : base(message, innerException)
    {
    }
}
