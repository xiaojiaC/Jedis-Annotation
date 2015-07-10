package redis.clients.util;

import java.io.Closeable;

import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

public abstract class Pool<T> implements Closeable {
  /* Apache Commons Pool2 源码分析:
   *    http://aofengblog.blog.163.com/blog/static/6317021201463075826473/
   */
  protected GenericObjectPool<T> internalPool;

  /**
   * Using this constructor means you have to set and initialize the internalPool yourself.
   */
  public Pool() {
  }

  @Override
  public void close() {
    closeInternalPool();
  }

  public boolean isClosed() {
    return this.internalPool.isClosed();
  }

  public Pool(final GenericObjectPoolConfig poolConfig, PooledObjectFactory<T> factory) {
    initPool(poolConfig, factory);
  }

  public void initPool(final GenericObjectPoolConfig poolConfig, PooledObjectFactory<T> factory) {

    if (this.internalPool != null) {
      try {
        closeInternalPool();
      } catch (Exception e) {
      }
    }

    this.internalPool = new GenericObjectPool<T>(factory, poolConfig);
  }

  // 从池中借出一个对象
  public T getResource() {
    try {
      return internalPool.borrowObject();
    } catch (Exception e) {
      throw new JedisConnectionException("Could not get a resource from the pool", e);
    }
  }

  // 将一个对象返还给池
  public void returnResourceObject(final T resource) {
    if (resource == null) {
      return;
    }
    try {
      internalPool.returnObject(resource);
    } catch (Exception e) {
      throw new JedisException("Could not return the resource to the pool", e);
    }
  }

  // 废弃一个对象
  public void returnBrokenResource(final T resource) {
    if (resource != null) {
      returnBrokenResourceObject(resource);
    }
  }

  public void returnResource(final T resource) {
    if (resource != null) {
      returnResourceObject(resource);
    }
  }

  public void destroy() {
    closeInternalPool();
  }

  protected void returnBrokenResourceObject(final T resource) {
    try {
      internalPool.invalidateObject(resource);
    } catch (Exception e) {
      throw new JedisException("Could not return the resource to the pool", e);
    }
  }

  // 关闭连接池
  protected void closeInternalPool() {
    try {
      internalPool.close();
    } catch (Exception e) {
      throw new JedisException("Could not destroy the pool", e);
    }
  }

  // 返回从池中借出的对象数量
  public int getNumActive() {
    if (this.internalPool == null || this.internalPool.isClosed()) {
      return -1;
    }

    return this.internalPool.getNumActive();
  }

  // 返回池中空闲的对象数量
  public int getNumIdle() {
    if (this.internalPool == null || this.internalPool.isClosed()) {
      return -1;
    }

    return this.internalPool.getNumIdle();
  }

  // 返回当前阻塞的线程数的估计数，等待一个对象从池中返回。
  public int getNumWaiters() {
    if (this.internalPool == null || this.internalPool.isClosed()) {
      return -1;
    }

    return this.internalPool.getNumWaiters();
  }
}
