﻿using RepoDb.Interfaces;

namespace RepoDb;

/// <summary>
/// A class that is being used to cache the resultsets of the query operations. This is the default class used by the <see cref="MemoryCache"/> object.
/// </summary>
/// <typeparam name="T">The type of the value.</typeparam>
public class CacheItem<T> : IExpirable
{
    /// <summary>
    /// Creates a new instance of <see cref="CacheItem"/> object.
    /// </summary>
    /// <param name="key">The key of the cache.</param>
    /// <param name="value">The value of the cache.</param>
    public CacheItem(string key,
        T value)
        : this(key,
              value,
              Constant.DefaultCacheItemExpirationInMinutes)
    { }

    /// <summary>
    /// Creates a new instance of <see cref="CacheItem"/> object.
    /// </summary>
    /// <param name="key">The key of the cache item.</param>
    /// <param name="value">The value of the cache item.</param>
    /// <param name="cacheItemExpiration">The expiration in minutes of the cache item.</param>
    public CacheItem(string key,
        T value,
        int cacheItemExpiration = Constant.DefaultCacheItemExpirationInMinutes)
    {
        if (cacheItemExpiration < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(cacheItemExpiration), "Expiration in minutes must not be negative values.");
        }
        Key = key;
        Value = value;
        CacheItemExpiration = cacheItemExpiration;
        CreatedDate = DateTime.UtcNow;
        Expiration = CreatedDate.AddMinutes(cacheItemExpiration);
    }

    #region Methods

    /// <summary>
    /// Updates the properties of the current item based from the passed item.
    /// </summary>
    /// <param name="item">The source item.</param>
    /// <param name="throwException">Throws an exception if the operation has failed to update an item.</param>
    internal void Update(CacheItem<T> item,
        bool throwException = true)
    {
        if (!IsExpired() && throwException)
        {
            throw new InvalidOperationException($"Cannot update the item that is not yet expired.");
        }

        Value = item.Value;
        CreatedDate = item.CreatedDate;
        Expiration = CreatedDate.AddMinutes(item.CacheItemExpiration ?? Constant.DefaultCacheItemExpirationInMinutes);
    }

    /// <summary>
    /// Updates the value of the current item.
    /// </summary>
    /// <param name="value">The actual value.</param>
    /// <param name="cacheItemExpiration">The expiration in minutes of the cache item.</param>
    /// <param name="throwException">Throws an exception if the operation has failed to update an item.</param>
    internal void Update(T value,
        int? cacheItemExpiration = Constant.DefaultCacheItemExpirationInMinutes,
        bool throwException = true)
    {
        if (!IsExpired() && throwException)
        {
            throw new InvalidOperationException($"Cannot update the item that is not yet expired.");
        }

        Value = value;
        CreatedDate = DateTime.UtcNow;
        Expiration = CreatedDate.AddMinutes(cacheItemExpiration.Value);
    }

    #endregion

    #region Properties

    /// <summary>
    /// Gets the key of the cache.
    /// </summary>
    public string Key { get; }

    /// <summary>
    /// Gets the value of the cache.
    /// </summary>
    public T Value { get; private set; }

    /// <summary>
    /// Gets the expiration in minutes of the cache item.
    /// </summary>
    public int? CacheItemExpiration { get; }

    #endregion

    #region IExpirable

    /// <summary>
    /// Gets the created timestamp of this cache item. By default, it is equals to the time
    /// of when this cache item object has been instantiated.
    /// </summary>
    public DateTime CreatedDate { get; private set; }

    /// <summary>
    /// Gets or sets the expiration date of this cache item.
    /// </summary>
    public DateTime Expiration { get; set; }

    /// <summary>
    /// Identifies whether this cache item is expired.
    /// </summary>
    /// <returns>A boolean value that indicate whether this cache item is expired.</returns>
    public bool IsExpired() => DateTime.UtcNow >= Expiration;

    #endregion
}
