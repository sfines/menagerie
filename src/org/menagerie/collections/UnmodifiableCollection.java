/*
 * Copyright 2010 Scott Fines
 * <p>
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.menagerie.collections;

import org.menagerie.Beta;

import java.util.Collection;

/**
 * TODO -sf- document!
 *
 * @author Scott Fines
 * @version 1.0
 *          Date: 11-Jan-2011
 *          Time: 09:08:09
 */
@Beta
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
