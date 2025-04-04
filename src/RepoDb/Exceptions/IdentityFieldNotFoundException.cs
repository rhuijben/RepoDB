namespace RepoDb.Exceptions;

/// <summary>
/// An exception that is being thrown if the identity key is not found from the data entity.
/// </summary>
public class IdentityFieldNotFoundException : Exception
{
    /// <summary>
    /// Creates a new instance of <see cref="IdentityFieldNotFoundException"/> class.
    /// </summary>
    /// <param name="message">The exception message.</param>
    public IdentityFieldNotFoundException(string message)
        : base(message) { }
    public IdentityFieldNotFoundException()
    {
    }
    public IdentityFieldNotFoundException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}
