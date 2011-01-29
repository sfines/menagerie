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

import java.util.*;

/**
 * Composite Iterator.
 * <p>
 * This iterator represents a single, over-arching iterator which may be comprised of several
 * sub-iterators, and is useful for transparently iterating over composite collections such as Hash-based
 * collections which use buckets.
 * <p>
 * Like many direct iterator implementations, this version is not thread-safe, and is intended for use
 * by only a single thread. However, it's consistency behavior is governed by the consistency behavior of the
 * underlying iterators that it works over. That is, if a single iterator is fail-fast, then this iterator must
 * be considered so as well. Only if <i>all</i> the underlying iterators are weakly-consistent does this iterator
 * become weakly-consistent.
 *
 * @author Scott Fines
 * @version 1.0
 */
final class CompositeIterator<T> implements Iterator<T> {
    private final Queue<Iterator<T>> iterators;

    public CompositeIterator(Iterator<T>...iterators) {
        this.iterators = new LinkedList<Iterator<T>>();
        this.iterators.addAll(Arrays.asList(iterators));
    }

    public CompositeIterator(Collection<? extends Iterator<T>> iterators) {
        this.iterators = new LinkedList<Iterator<T>>(iterators);
    }

    @Override
    public boolean hasNext() {
        if(iterators.size()<=0)return false;

        while(iterators.size()>0){
            Iterator<T> iterator = iterators.peek();
            if(iterator.hasNext())return true;
            else{
                iterators.poll();
            }
        }
        return false;
    }

    @Override
    public T next() {
        if(iterators.size()<=0)throw new NoSuchElementException();
        Iterator<T> iterator = iterators.peek();
        return iterator.next();
    }


    @Override
    public void remove() {
        if(iterators.size()<=0) throw new IllegalStateException("This iterator has been exhausted and cannot be removed");

        //call remove on that iterator
        Iterator<T> iterator = iterators.peek();
        iterator.remove();
    }

}
