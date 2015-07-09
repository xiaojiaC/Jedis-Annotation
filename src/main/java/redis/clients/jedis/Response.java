package redis.clients.jedis;

import redis.clients.jedis.exceptions.JedisDataException;

/**
 * <pre>
 * Transaction t = jedis.multi();
 * t.set("fool", "bar"); 
 * Response<String> result1 = t.get("fool");

 * t.zadd("foo", 1, "barowitch"); 
 * t.zadd("foo", 0, "barinsky"); 
 * Response<Set<String>> sose = t.zrange("foo", 0, -1);   // get the entire sortedset
 * t.exec();                                              // don't forget it

 * String foolbar = result1.get();                        // use Response.get() to retrieve things from a Response
 * int soseSize = sose.get().size();                      // on sose.get() you can directly call Set methods!

 * // List<Object> allResults = t.exec();              // you could still get all results at once, as before
 * </pre>
 */
public class Response<T> {
  protected T response = null;
  protected JedisDataException exception = null;

  // 是否正在building
  private boolean building = false;
  // 已被build
  private boolean built = false;
  // response是否被设置数据
  private boolean set = false;

  // 数据构建器
  private Builder<T> builder;
  // 设置响应数据
  private Object data;
  // 依赖的响应
  private Response<?> dependency = null;

  public Response(Builder<T> b) {
    this.builder = b;
  }

  public void set(Object data) {
    this.data = data;
    set = true;
  }

  public T get() {
    // if response has dependency response and dependency is not built,
    // build it first and no more!!
    if (dependency != null && dependency.set && !dependency.built) {
      dependency.build();
    }
    if (!set) {
      throw new JedisDataException(
          "Please close pipeline or multi block before calling this method.");
    }
    if (!built) {
      build();
    }
    if (exception != null) {
      throw exception;
    }
    return response;
  }

  public void setDependency(Response<?> dependency) {
    this.dependency = dependency;
  }

  private void build() {
    // check build state to prevent recursion
    if (building) {
      return;
    }

    building = true;
    try {
      if (data != null) {
        if (data instanceof JedisDataException) {
          exception = (JedisDataException) data;
        } else {
          response = builder.build(data);
        }
      }

      data = null;
    } finally {
      building = false;
      built = true;
    }
  }

  public String toString() {
    return "Response " + builder.toString();
  }

}
