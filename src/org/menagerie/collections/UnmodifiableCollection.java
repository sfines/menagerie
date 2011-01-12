package org.menagerie.collections;

import java.util.Collection;

/**
 * TODO -sf- document!
 *
 * @author Scott Fines
 * @version 1.0
 *          Date: 11-Jan-2011
 *          Time: 09:08:09
 */
abstract class UnmodifiableCollection<T> implements Collection<T> {

    @Override
    public final void clear() {
        throw new UnsupportedOperationException("Removing elements are not supported by this collection");
    }

    @Override
    public final boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException("Removing elements are not supported by this collection");
    }

    @Override
    public final boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException("Removing elements are not supported by this collection");
    }

    @Override
    public final boolean addAll(Collection<? extends T> c) {
        throw new UnsupportedOperationException("Adding elements are not supported by this collection");
    }

    @Override
    public final boolean remove(Object o) {
        throw new UnsupportedOperationException("Removing elements are not supported by this collection");
    }

    @Override
    public final boolean add(T t) {
        throw new UnsupportedOperationException("Adding elements are not supported by this collection");
    }
}
