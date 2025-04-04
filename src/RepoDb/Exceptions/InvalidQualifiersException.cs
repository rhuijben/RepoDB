namespace RepoDb.Exceptions;

/// <summary>
/// An exception that is being thrown if the qualifier <see cref="Field"/> objects passed in the operation are not valid.
/// </summary>
public class InvalidQualifiersException : ArgumentOutOfRangeException
{
    /// <summary>
    /// Creates a new instance of <see cref="InvalidQualifiersException"/> class.
    /// </summary>
    /// <param name="message">The exception message.</param>
    public InvalidQualifiersException(string? message)
        : base(message: message, innerException: null) { }
    public InvalidQualifiersException()
    {
    }
    public InvalidQualifiersException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}
