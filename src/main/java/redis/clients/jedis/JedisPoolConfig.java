package redis.clients.jedis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

public class JedisPoolConfig extends GenericObjectPoolConfig {
  /*
   * JedisPoolConfig配置:
   *    http://www.cnblogs.com/tankaixiong/p/4048167.html
   */
  public JedisPoolConfig() {
    // defaults to make your life with connection pool easier :)

    // 在空闲时检查有效性
    setTestWhileIdle(true);
    // 逐出连接的最小空闲时间
    setMinEvictableIdleTimeMillis(60000);
    // 逐出扫描的时间间隔(毫秒) 如果为负数,则不运行逐出线程
    setTimeBetweenEvictionRunsMillis(30000);
    // 每次逐出检查时,逐出的最大数目 如果为负数就是 : 1/abs(n)
    setNumTestsPerEvictionRun(-1);
  }
}
