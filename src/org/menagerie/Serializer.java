package org.menagerie;

/**
 * TODO -sf- document!
 *
 * @author Scott Fines
 * @version 1.0
 *          Date: 06-Jan-2011
 *          Time: 21:10:39
 */
@Beta
public interface Serializer<T> {

    public byte[] serialize(T instance);

    public T deserialize(byte[] data);
}
