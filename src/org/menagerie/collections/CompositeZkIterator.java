package org.menagerie.collections;

import java.util.*;

/**
 * TODO -sf- document!
 *
 * @author Scott Fines
 * @version 1.0
 *          Date: 11-Jan-2011
 *          Time: 09:10:59
 */
class CompositeZkIterator<T> implements Iterator<T> {
    private final Queue<ZkIterator<T>> iterators;
    private volatile boolean initialized = false;

    public CompositeZkIterator(ZkIterator<T>...iterators) {
        this.iterators = new LinkedList<ZkIterator<T>>();
        this.iterators.addAll(Arrays.asList(iterators));
    }

    public CompositeZkIterator(List<ZkIterator<T>> iterators) {
        this.iterators = new LinkedList<ZkIterator<T>>(iterators);
    }

    public synchronized void initIterator(){
        if(initialized) throw new IllegalArgumentException("Initialization has been called twice!");
        if(iterators.size()>0)
            iterators.peek().initIterator();
        initialized = true;
    }

    @Override
    public synchronized boolean hasNext() {
        return iterators.size() > 0 && iterators.peek().hasNext();

    }

    @Override
    public synchronized T next() {
        if(iterators.size()<=0)throw new NoSuchElementException();
        ZkIterator<T> iterator = iterators.peek();
        T t = iterator.next();
        advance(iterator);
        return t;
    }


    @Override
    public synchronized void remove() {
        throw new UnsupportedOperationException("Removal not supported by this iterator");
    }

    private void advance(ZkIterator<T> iterator) {
        if(!iterator.hasNext()){
            //done with this iterator
            iterator.closeIterator();
            iterators.remove();
            if(iterators.size()>0)
                iterators.peek().initIterator();
        }
    }

}
