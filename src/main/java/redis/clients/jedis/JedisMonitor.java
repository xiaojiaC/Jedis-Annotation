package redis.clients.jedis;

/**
 * 实时打印出 Redis 服务器接收到的命令，调试用。
 */
public abstract class JedisMonitor {
  protected Client client;

  public void proceed(Client client) {
    this.client = client;
    this.client.setTimeoutInfinite();
    do {
      String command = client.getBulkReply();
      onCommand(command);
    } while (client.isConnected());
  }

  public abstract void onCommand(String command);
}