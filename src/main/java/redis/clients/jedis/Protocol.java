package redis.clients.jedis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import redis.clients.jedis.exceptions.JedisAskDataException;
import redis.clients.jedis.exceptions.JedisClusterException;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisMovedDataException;
import redis.clients.util.RedisInputStream;
import redis.clients.util.RedisOutputStream;
import redis.clients.util.SafeEncoder;

public final class Protocol {

  private static final String ASK_RESPONSE = "ASK";
  private static final String MOVED_RESPONSE = "MOVED";
  private static final String CLUSTERDOWN_RESPONSE = "CLUSTERDOWN";
  public static final String DEFAULT_HOST = "localhost";
  public static final int DEFAULT_PORT = 6379;
  public static final int DEFAULT_SENTINEL_PORT = 26379;
  public static final int DEFAULT_TIMEOUT = 2000;
  public static final int DEFAULT_DATABASE = 0;

  public static final String CHARSET = "UTF-8";

  public static final byte DOLLAR_BYTE = '$';
  public static final byte ASTERISK_BYTE = '*';
  public static final byte PLUS_BYTE = '+';
  public static final byte MINUS_BYTE = '-';
  public static final byte COLON_BYTE = ':';

  public static final String SENTINEL_MASTERS = "masters";
  public static final String SENTINEL_GET_MASTER_ADDR_BY_NAME = "get-master-addr-by-name";
  public static final String SENTINEL_RESET = "reset";
  public static final String SENTINEL_SLAVES = "slaves";
  public static final String SENTINEL_FAILOVER = "failover";
  public static final String SENTINEL_MONITOR = "monitor";
  public static final String SENTINEL_REMOVE = "remove";
  public static final String SENTINEL_SET = "set";

  public static final String CLUSTER_NODES = "nodes";
  public static final String CLUSTER_MEET = "meet";
  public static final String CLUSTER_RESET = "reset";
  public static final String CLUSTER_ADDSLOTS = "addslots";
  public static final String CLUSTER_DELSLOTS = "delslots";
  public static final String CLUSTER_INFO = "info";
  public static final String CLUSTER_GETKEYSINSLOT = "getkeysinslot";
  public static final String CLUSTER_SETSLOT = "setslot";
  public static final String CLUSTER_SETSLOT_NODE = "node";
  public static final String CLUSTER_SETSLOT_MIGRATING = "migrating";
  public static final String CLUSTER_SETSLOT_IMPORTING = "importing";
  public static final String CLUSTER_SETSLOT_STABLE = "stable";
  public static final String CLUSTER_FORGET = "forget";
  public static final String CLUSTER_FLUSHSLOT = "flushslots";
  public static final String CLUSTER_KEYSLOT = "keyslot";
  public static final String CLUSTER_COUNTKEYINSLOT = "countkeysinslot";
  public static final String CLUSTER_SAVECONFIG = "saveconfig";
  public static final String CLUSTER_REPLICATE = "replicate";
  public static final String CLUSTER_SLAVES = "slaves";
  public static final String CLUSTER_FAILOVER = "failover";
  public static final String CLUSTER_SLOTS = "slots";
  public static final String PUBSUB_CHANNELS = "channels";
  public static final String PUBSUB_NUMSUB = "numsub";
  public static final String PUBSUB_NUM_PAT = "numpat";

  public static final byte[] BYTES_TRUE = toByteArray(1);
  public static final byte[] BYTES_FALSE = toByteArray(0);

  private Protocol() {
    // this prevent the class from instantiation
  }

  // 发送二进制命令
  public static void sendCommand(final RedisOutputStream os, final ProtocolCommand command,
      final byte[]... args) {
    sendCommand(os, command.getRaw(), args);
  }

  /** 
   * <pre>
   * 发送命令，遵循redis协议格式
   * *<参数数量> CR LF
   * $<参数 1 的字节数量> CR LF
   * <参数 1 的数据> CR LF
   * ...
   * $<参数 N 的字节数量> CR LF
   * <参数 N 的数据> CR LF
   * </pre>
   */
  private static void sendCommand(final RedisOutputStream os, final byte[] command,
      final byte[]... args) {
    try {
      os.write(ASTERISK_BYTE);
      os.writeIntCrLf(args.length + 1);
      os.write(DOLLAR_BYTE);
      os.writeIntCrLf(command.length);
      os.write(command);
      os.writeCrLf();

      for (final byte[] arg : args) {
        os.write(DOLLAR_BYTE);
        os.writeIntCrLf(arg.length);
        os.write(arg);
        os.writeCrLf();
      }
    } catch (IOException e) {
      throw new JedisConnectionException(e);
    }
  }

  /**
   * <pre>
   * 错误回复只在某些地方出现问题时发送： 比如说， 当用户对不正确的数据类型执行命令， 或者执行一个不存在的命令。
   * ERR 是一个通用错误，而 WRONGTYPE 则是一个更特定的错误。 一个客户端实现可以为不同类型的错误产生不同类型的异常，
   * 或者提供一种通用的方式， 让调用者可以通过提供字符串形式的错误名来捕捉（trap）不同的错误。
   * eg:
   *     cli: incr name
   *     server: -ERR value is not an integer or out of range
   * 
   * 集群节点不能代理（proxy）命令请求，所以客户端应该在节点返回 -MOVED 或者 -ASK 转向（redirection）
   * 错误时，自行将命令请求转发至其他节点。
   * eg：
   *    MOVED 3999 127.0.0.1:6381
   * 
   * 当集群不可用时,所有对集群的操作做都不可用，收到((error) CLUSTERDOWN The cluster is down)错误
   * 参考：
   *    http://blog.chinaunix.net/uid-7374279-id-4470290.html
   * </pre>
   */
  private static void processError(final RedisInputStream is) {
    String message = is.readLine();
    // TODO: I'm not sure if this is the best way to do this.
    // Maybe Read only first 5 bytes instead?
    if (message.startsWith(MOVED_RESPONSE)) {
      String[] movedInfo = parseTargetHostAndSlot(message);
      throw new JedisMovedDataException(message, new HostAndPort(movedInfo[1],
          Integer.valueOf(movedInfo[2])), Integer.valueOf(movedInfo[0]));
    } else if (message.startsWith(ASK_RESPONSE)) {
      String[] askInfo = parseTargetHostAndSlot(message);
      throw new JedisAskDataException(message, new HostAndPort(askInfo[1],
          Integer.valueOf(askInfo[2])), Integer.valueOf(askInfo[0]));
    } else if (message.startsWith(CLUSTERDOWN_RESPONSE)) {
      throw new JedisClusterException(message);
    }
    throw new JedisDataException(message);
  }

  private static String[] parseTargetHostAndSlot(String clusterRedirectResponse) {
    String[] response = new String[3];
    String[] messageInfo = clusterRedirectResponse.split(" ");
    String[] targetHostAndPort = messageInfo[2].split(":");
    response[0] = messageInfo[1];
    response[1] = targetHostAndPort[0];
    response[2] = targetHostAndPort[1];
    return response;
  }

  /**
   * <pre>
   * "+": 状态回复（status reply）         PLUS_BYTE
   * "-": 错误回复（error reply）          MINUS_BYTE
   * ":": 整数回复（integer reply）        COLON_BYTE
   * "$": 批量回复（bulk reply）           DOLLAR_BYTE
   * "*": 多条批量回复（multi bulk reply）  ASTERISK_BYTE
   * </pre>
   */
  private static Object process(final RedisInputStream is) {

    final byte b = is.readByte();
    if (b == PLUS_BYTE) {
      return processStatusCodeReply(is);
    } else if (b == DOLLAR_BYTE) {
      return processBulkReply(is);
    } else if (b == ASTERISK_BYTE) {
      return processMultiBulkReply(is);
    } else if (b == COLON_BYTE) {
      return processInteger(is);
    } else if (b == MINUS_BYTE) {
      processError(is);
      return null;
    } else {
      throw new JedisConnectionException("Unknown reply: " + (char) b);
    }
  }

  /**
   * <pre>
   * 状态回复通常由那些不需要返回数据的命令返回，这种回复不能包含新行。
   * eg:
   *    cli: set name zhangsan
   *    server: +OK
   * </pre>
   */
  private static byte[] processStatusCodeReply(final RedisInputStream is) {
    return is.readLineBytes();
  }

  /**
   * <pre>
   * 服务器使用批量回复来返回二进制安全的字符串，字符串的最大长度为 512 MB。
   *  eg:
   *      cli: get name
   *      server: $8\r\nzhangsan\r\n
   *  空批量回复：
   *  如果被请求的值不存在， 那么批量回复会将特殊值 -1 用作回复的长度值。当请求对象不存在时，客户端应该返回空对象，而不是空字符串。
   *  </pre>
   */
  private static byte[] processBulkReply(final RedisInputStream is) {
    final int len = is.readIntCrLf();
    if (len == -1) {
      return null;
    }

    final byte[] read = new byte[len];
    int offset = 0;
    while (offset < len) {
      final int size = is.read(read, offset, (len - offset));
      if (size == -1) throw new JedisConnectionException(
          "It seems like server has closed the connection.");
      offset += size;
    }

    // read 2 more bytes for the command delimiter
    is.readByte();
    is.readByte();

    return read;
  }

  /**
   * <pre>
   *  整数回复就是一个以 ":" 开头， CRLF 结尾的字符串表示的整数。
   *    eg:
   *    cli: exists name
   *    server: :1
   * </pre>
   */
  private static Long processInteger(final RedisInputStream is) {
    return is.readLongCrLf();
  }

  /**
   * <pre>
   * 多条批量回复是由多个回复组成的数组， 数组中的每个元素都可以是任意类型的回复， 包括多条批量回复本身。
   * eg:
   *    cli: lrange mylist 0 3
   *    server: *4\r\n
   *            :1\r\n
   *            :2\r\n
   *            :3\r\n
   *            $3\r\n
   *            foo\r\n
   * 多条批量回复也可以是空白的,
   * eg:
   *    cli: lrange mylist 7 8
   *    server: *0\r\n
   * 无内容的多条批量回复（null multi bulk reply）也是存在的， 比如当 BLPOP 命令的阻塞时间超过最大时限时， 它就返回一个无内容的多条批量回复， 这个回复的计数值为 -1 ：
   * eg:
   *    cli: blpop key 1
   *    server: *-1\r\n
   * 多条批量回复中的元素可以将自身的长度设置为 -1 ， 从而表示该元素不存在， 并且也不是一个空白字符串（empty string）。
   * </pre>
   */
  private static List<Object> processMultiBulkReply(final RedisInputStream is) {
    final int num = is.readIntCrLf();
    if (num == -1) {
      return null;
    }
    final List<Object> ret = new ArrayList<Object>(num);
    for (int i = 0; i < num; i++) {
      try {
        ret.add(process(is));
      } catch (JedisDataException e) {
        ret.add(e);
      }
    }
    return ret;
  }

  /**
   * 解析服务器返回的数据流
   */
  public static Object read(final RedisInputStream is) {
    return process(is);
  }

  public static final byte[] toByteArray(final boolean value) {
    return value ? BYTES_TRUE : BYTES_FALSE;
  }

  public static final byte[] toByteArray(final int value) {
    return SafeEncoder.encode(String.valueOf(value));
  }

  public static final byte[] toByteArray(final long value) {
    return SafeEncoder.encode(String.valueOf(value));
  }

  public static final byte[] toByteArray(final double value) {
    return SafeEncoder.encode(String.valueOf(value));
  }

  public static enum Command implements ProtocolCommand {
    PING, SET, GET, QUIT, EXISTS, DEL, TYPE, FLUSHDB, KEYS, RANDOMKEY, RENAME, RENAMENX, RENAMEX, DBSIZE, EXPIRE, EXPIREAT, TTL, SELECT, MOVE, FLUSHALL, GETSET, MGET, SETNX, SETEX, MSET, MSETNX, DECRBY, DECR, INCRBY, INCR, APPEND, SUBSTR, HSET, HGET, HSETNX, HMSET, HMGET, HINCRBY, HEXISTS, HDEL, HLEN, HKEYS, HVALS, HGETALL, RPUSH, LPUSH, LLEN, LRANGE, LTRIM, LINDEX, LSET, LREM, LPOP, RPOP, RPOPLPUSH, SADD, SMEMBERS, SREM, SPOP, SMOVE, SCARD, SISMEMBER, SINTER, SINTERSTORE, SUNION, SUNIONSTORE, SDIFF, SDIFFSTORE, SRANDMEMBER, ZADD, ZRANGE, ZREM, ZINCRBY, ZRANK, ZREVRANK, ZREVRANGE, ZCARD, ZSCORE, MULTI, DISCARD, EXEC, WATCH, UNWATCH, SORT, BLPOP, BRPOP, AUTH, SUBSCRIBE, PUBLISH, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, PUBSUB, ZCOUNT, ZRANGEBYSCORE, ZREVRANGEBYSCORE, ZREMRANGEBYRANK, ZREMRANGEBYSCORE, ZUNIONSTORE, ZINTERSTORE, ZLEXCOUNT, ZRANGEBYLEX, ZREVRANGEBYLEX, ZREMRANGEBYLEX, SAVE, BGSAVE, BGREWRITEAOF, LASTSAVE, SHUTDOWN, INFO, MONITOR, SLAVEOF, CONFIG, STRLEN, SYNC, LPUSHX, PERSIST, RPUSHX, ECHO, LINSERT, DEBUG, BRPOPLPUSH, SETBIT, GETBIT, BITPOS, SETRANGE, GETRANGE, EVAL, EVALSHA, SCRIPT, SLOWLOG, OBJECT, BITCOUNT, BITOP, SENTINEL, DUMP, RESTORE, PEXPIRE, PEXPIREAT, PTTL, INCRBYFLOAT, PSETEX, CLIENT, TIME, MIGRATE, HINCRBYFLOAT, SCAN, HSCAN, SSCAN, ZSCAN, WAIT, CLUSTER, ASKING, PFADD, PFCOUNT, PFMERGE;

    private final byte[] raw;

    Command() {
      raw = SafeEncoder.encode(this.name());
    }

    @Override
    public byte[] getRaw() {
      return raw;
    }
  }

  public static enum Keyword {
    AGGREGATE, ALPHA, ASC, BY, DESC, GET, LIMIT, MESSAGE, NO, NOSORT, PMESSAGE, PSUBSCRIBE, PUNSUBSCRIBE, OK, ONE, QUEUED, SET, STORE, SUBSCRIBE, UNSUBSCRIBE, WEIGHTS, WITHSCORES, RESETSTAT, RESET, FLUSH, EXISTS, LOAD, KILL, LEN, REFCOUNT, ENCODING, IDLETIME, AND, OR, XOR, NOT, GETNAME, SETNAME, LIST, MATCH, COUNT;
    public final byte[] raw;

    Keyword() {
      raw = SafeEncoder.encode(this.name().toLowerCase());
    }
  }
}
