package redis.clients.jedis;

import static redis.clients.jedis.Protocol.Keyword.MESSAGE;
import static redis.clients.jedis.Protocol.Keyword.PMESSAGE;
import static redis.clients.jedis.Protocol.Keyword.PSUBSCRIBE;
import static redis.clients.jedis.Protocol.Keyword.PUNSUBSCRIBE;
import static redis.clients.jedis.Protocol.Keyword.SUBSCRIBE;
import static redis.clients.jedis.Protocol.Keyword.UNSUBSCRIBE;

import java.util.Arrays;
import java.util.List;

import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.SafeEncoder;

/**
 * 发布订阅(pub/sub)是一种消息通信模式,主要的目的是解耦消息发布者和消息订阅者之间的耦合,这点和设计模式中的观察者模式比较相似。
 * pub/sub 不仅仅解决发布者和订阅者直接代码级别耦合也解决两者在物理部署上的耦合。redis 作为一个 pub/sub 的 server,在订阅
 * 者和发布者之间起到了消息路由的功能。订阅者可以通过 subscribe 和 psubscribe 命令向 redis server订阅自己感兴趣的消息类型
 * ,redis 将消息类型称为通道(channel)。当发布者通过publish 命令向 redis server 发送特定类型的消息时。订阅该消息类型的全部
 * client 都会收到此消息。这里消息的传递是多对多的。一个 client 可以订阅多个 channel,也可以向多个 channel发送消息。
 *
 * 频道转发的每条信息都是一条带有三个元素的多条批量回复（multi-bulk reply）。
 *
 * 信息的第一个元素标识了信息的类型：
 *    -subscribe ： 表示当前客户端成功地订阅了信息第二个元素所指示的频道。 而信息的第三个元素则记录了目前客户端已订阅频道的总数。
 *    -unsubscribe ： 表示当前客户端成功地退订了信息第二个元素所指示的频道。 信息的第三个元素记录了客户端目前仍在订阅的频道数量。
 *    当客户端订阅的频道数量降为 0 时， 客户端不再订阅任何频道， 它可以像往常一样， 执行任何 Redis 命令。
 *    message ： 表示这条信息是由某个客户端执行 PUBLISH 命令所发送的， 真正的信息。 信息的第二个元素是信息来源的频道， 而第三个
 *    元素则是信息的内容。
 *
 * 订阅模式:
 *   Redis 的发布与订阅实现支持模式匹配（pattern matching）： 客户端可以订阅一个带 * 号的模式， 如果某个/某些频道的名字和这个模式匹配， 那么当有信息发送给这个/这些频道的时候， 客户端也会收到这个/这些频道的信息。
 * 
 *   通过订阅模式接收到的信息， 和通过订阅频道接收到的信息， 这两者的格式不太一样：
 *      通过订阅模式而接收到的信息的类型为pmessage：这代表有某个客户端通过PUBLISH向某个频道发送了信息，而这个频道刚好匹配了当前客户端所订阅的某个模式。 信息的第二个元素记录了被匹配的模式， 第三个元素记录了被匹配的频道的名字， 最后一个元素则记录了信息的实际内容。
 *
 *  客户端处理 PSUBSCRIBE 和 PUNSUBSCRIBE 返回值的方式， 和客户端处理 SUBSCRIBE 和 UNSUBSCRIBE 的方式类似： 通过对信息的第一个元素进行分析， 客户端可以判断接收到的信息是一个真正的信息， 还是 PSUBSCRIBE 或 PUNSUBSCRIBE 命令的返回值。
 */
public abstract class JedisPubSub {
  private int subscribedChannels = 0;
  private volatile Client client;

  public void onMessage(String channel, String message) {
  }

  public void onPMessage(String pattern, String channel, String message) {
  }

  public void onSubscribe(String channel, int subscribedChannels) {
  }

  public void onUnsubscribe(String channel, int subscribedChannels) {
  }

  public void onPUnsubscribe(String pattern, int subscribedChannels) {
  }

  public void onPSubscribe(String pattern, int subscribedChannels) {
  }

  public void unsubscribe() {
    if (client == null) {
      throw new JedisConnectionException("JedisPubSub was not subscribed to a Jedis instance.");
    }
    client.unsubscribe();
    client.flush();
  }

  public void unsubscribe(String... channels) {
    if (client == null) {
      throw new JedisConnectionException("JedisPubSub is not subscribed to a Jedis instance.");
    }
    client.unsubscribe(channels);
    client.flush();
  }

  public void subscribe(String... channels) {
    if (client == null) {
      throw new JedisConnectionException("JedisPubSub is not subscribed to a Jedis instance.");
    }
    client.subscribe(channels);
    client.flush();
  }

  public void psubscribe(String... patterns) {
    if (client == null) {
      throw new JedisConnectionException("JedisPubSub is not subscribed to a Jedis instance.");
    }
    client.psubscribe(patterns);
    client.flush();
  }

  public void punsubscribe() {
    if (client == null) {
      throw new JedisConnectionException("JedisPubSub is not subscribed to a Jedis instance.");
    }
    client.punsubscribe();
    client.flush();
  }

  public void punsubscribe(String... patterns) {
    if (client == null) {
      throw new JedisConnectionException("JedisPubSub is not subscribed to a Jedis instance.");
    }
    client.punsubscribe(patterns);
    client.flush();
  }

  public boolean isSubscribed() {
    return subscribedChannels > 0;
  }

  public void proceedWithPatterns(Client client, String... patterns) {
    this.client = client;
    client.psubscribe(patterns);
    client.flush();
    process(client);
  }

  public void proceed(Client client, String... channels) {
    this.client = client;
    client.subscribe(channels);
    client.flush();
    process(client);
  }

  private void process(Client client) {

    do {
      List<Object> reply = client.getRawObjectMultiBulkReply();
      final Object firstObj = reply.get(0);
      if (!(firstObj instanceof byte[])) {
        throw new JedisException("Unknown message type: " + firstObj);
      }
      final byte[] resp = (byte[]) firstObj;
      if (Arrays.equals(SUBSCRIBE.raw, resp)) {
        subscribedChannels = ((Long) reply.get(2)).intValue();
        final byte[] bchannel = (byte[]) reply.get(1);
        final String strchannel = (bchannel == null) ? null : SafeEncoder.encode(bchannel);
        onSubscribe(strchannel, subscribedChannels);
      } else if (Arrays.equals(UNSUBSCRIBE.raw, resp)) {
        subscribedChannels = ((Long) reply.get(2)).intValue();
        final byte[] bchannel = (byte[]) reply.get(1);
        final String strchannel = (bchannel == null) ? null : SafeEncoder.encode(bchannel);
        onUnsubscribe(strchannel, subscribedChannels);
      } else if (Arrays.equals(MESSAGE.raw, resp)) {
        final byte[] bchannel = (byte[]) reply.get(1);
        final byte[] bmesg = (byte[]) reply.get(2);
        final String strchannel = (bchannel == null) ? null : SafeEncoder.encode(bchannel);
        final String strmesg = (bmesg == null) ? null : SafeEncoder.encode(bmesg);
        onMessage(strchannel, strmesg);
      } else if (Arrays.equals(PMESSAGE.raw, resp)) {
        final byte[] bpattern = (byte[]) reply.get(1);
        final byte[] bchannel = (byte[]) reply.get(2);
        final byte[] bmesg = (byte[]) reply.get(3);
        final String strpattern = (bpattern == null) ? null : SafeEncoder.encode(bpattern);
        final String strchannel = (bchannel == null) ? null : SafeEncoder.encode(bchannel);
        final String strmesg = (bmesg == null) ? null : SafeEncoder.encode(bmesg);
        onPMessage(strpattern, strchannel, strmesg);
      } else if (Arrays.equals(PSUBSCRIBE.raw, resp)) {
        subscribedChannels = ((Long) reply.get(2)).intValue();
        final byte[] bpattern = (byte[]) reply.get(1);
        final String strpattern = (bpattern == null) ? null : SafeEncoder.encode(bpattern);
        onPSubscribe(strpattern, subscribedChannels);
      } else if (Arrays.equals(PUNSUBSCRIBE.raw, resp)) {
        subscribedChannels = ((Long) reply.get(2)).intValue();
        final byte[] bpattern = (byte[]) reply.get(1);
        final String strpattern = (bpattern == null) ? null : SafeEncoder.encode(bpattern);
        onPUnsubscribe(strpattern, subscribedChannels);
      } else {
        throw new JedisException("Unknown message type: " + firstObj);
      }
    } while (isSubscribed());

    /* Invalidate instance since this thread is no longer listening */
    this.client = null;

    /*
     * Reset pipeline count because subscribe() calls would have increased it but nothing
     * decremented it.
     */
    client.resetPipelinedCount();
  }

  public int getSubscribedChannels() {
    return subscribedChannels;
  }
}
