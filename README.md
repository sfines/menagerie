Welcome to Menagerie
==========================================

What is Menagerie?
-----------------

Menagerie is an implementation of the Java Concurrency Libraries based on the popular Apache ZooKeeper(http://zookeeper.apache.org).

Why Java Concurrency?
--------------------

Many Java programmers are familiar with the concurrency model derived in the java.util.concurrent.* packages, but the distributed realm lacks most of these familiar tools. With Menagerie, applications written with java concurrency in mind can become a distributed application with less effort.

Why ZooKeeper?
--------------

One of the most difficult aspects of distributed programming is the need to coordinate activities between different parties which are physically separated. They may be different machines running the same application, different applications using the same core services, even different language implementations, but they are all separated. The challenge is to coordinate all these different systems together so as not to ruin or disturb the core of the applications. ZooKeeper is one of the few systems which addresses this problem.

How to use Menagerie
-------------------

Menagerie is very simple to use, if you are familiar with the java.util.concurrent tools. Wherever you would use a java.util.concurrent tool, drop in the corresponding Menagerie tool instead. Take, for example, a lock. In a java.util.concurrent application, you would do something like:

<code>
	Lock myLock = new ReentrantLock();
	myLock.lock();
	try{
		//do your stuff
	}finally{
		myLock.unlock();
	}
</code>

With Menagerie, you would do:

<code>
	ZkSessionManager sessionManager = new DefaultZkSessionManager("zookeeperserverslist",zooKeeperTimeout);
	Lock myLock = new ReentrantZkLock("/<path to my lock node>",sessionManager);
	myLock.lock();
	try{
		//do your stuff
	}finally{
		myLock.unlock();
	}
</code>

And the same applies for all the Menagerie tools. 

Wherever java.util.concurrent defines an interface, we either do already, or plan to in the near future, provide an implemenation of it based on ZooKeeper. Of course, many of the java.util.concurrent tools aren't defined as interfaces (CountDownLatch, Semaphore, and CyclicBarrier to name a few). In those cases, we provide an API which is as similar to that of java.util.concurrent as reasonably possible, to ease the transition into ZooKeeper-based development.

Road Map:
---------

Obviously, Menagerie doesn't have every concurrency tool ever written, though we plan to. Initially, Menagerie contains 

* Reentrant Mutex Locks
* Reentrant ReadWrite Locks
* Distributed Lock Conditions
* Count Down Latches
* Cyclic Barriers
* Synchronous Leader Election

ZooKeeper is capable of much more, and we plan to do the following in the next few releases:

* Asynchronous Leader Election
* Semaphores
* AbstractZkQueuedSynchronizer
* Distributed Event framework
* Distributed Executor framework

If you think of something that you would like to see, please drop us a line so we can add it to the road map!

Commit Back!
------------

If you see a bug, think of an issue, or just would like to make some commentary, please do! We appreciate all the help that we can get. Just fork and send us a pull request on github.

Contributors
-----------

Scott Fines
