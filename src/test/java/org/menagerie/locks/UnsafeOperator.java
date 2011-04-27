package org.menagerie.locks;

/**
 * Test utility specifically aimed at measuring lock quality.
 *
 * <p>This class is explicitly designed to be <em>not</em> Thread-safe.
 * That way, tests which involve this class will fail if locks are
 * not guarding this instance appropriately.
 * 
 * @author Scott Fines
 *         Date: Apr 26, 2011
 *         Time: 9:57:43 PM
 */
public final class UnsafeOperator{
    private int operator;

    public void increment(){
        operator++;
    }

    public int getValue(){
        return operator;
    }
}
