package org.menagerie.collections;

import java.util.Set;

/**
 * TODO -sf- document!
 *
 * @author Scott Fines
 * @version 1.0
 *          Date: 11-Jan-2011
 *          Time: 16:52:02
 */
public interface ZkSet<T> extends Set<T> {

    ZkIterator<T> zkIterator();
}
