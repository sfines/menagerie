package org.menagerie.collections;

import java.util.*;

/**
 * TODO -sf- document!
 *
 * @author Scott Fines
 * @version 1.0
 *          Date: 11-Jan-2011
 *          Time: 09:57:34
 */
class CompositeZkSetView<E> extends UnmodifiableCollection<E> implements Set<E> {
    private final List<ZkSet<E>> sets;

    CompositeZkSetView(List<ZkSet<E>> sets) {
        this.sets = sets;
    }

    @Override
    public int size() {
        int size = 0;
        for(Set<E> set:sets){
            size+=set.size();
        }
        return size;
    }

    @Override
    public boolean isEmpty() {
        for(Set<E> set:sets){
            if(!set.isEmpty())return false;
        }
        return true;
    }

    @Override
    public boolean contains(Object o) {
        for(Set<E> set:sets){
            if(set.contains(o))return true;
        }
        return false;
    }

    @Override
    public Iterator<E> iterator() {
        List<ZkIterator<E>>iterators = new ArrayList<ZkIterator<E>>(sets.size());
        for(ZkSet<E> set:sets){
            iterators.add(set.zkIterator());
        }
        return new CompositeZkIterator<E>(iterators);
    }

    @Override
    public Object[] toArray() {
        List<Object[]> items = new ArrayList<Object[]>();
        for(Set<E> set:sets){
            items.add(set.toArray());
        }
        return items.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        List<T[]> items = new ArrayList<T[]>();
        for(Set<E> set:sets){
            items.add(set.toArray(a));
        }
        return items.toArray(a);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        for(Object o:c){
            if(!contains(o))return false;
        }
        return true;
    }


}
