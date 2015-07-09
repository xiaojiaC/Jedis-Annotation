package redis.clients.jedis;

import java.util.ArrayList;
import java.util.List;

import redis.clients.jedis.exceptions.JedisDataException;

/**
 * Transaction is nearly identical to Pipeline, only differences are the multi/discard behaviors
 */
/**
 * redis 对事务的支持目前还比较简单。redis 只能保证一个 client 发起的事务中的命令可以连续的执行,而中间不会插入其他 client 的命令。
 * 由于 redis 是单线程来处理所有 client 的请求的所以做到这点是很容易的。一般情况下 redis 在接受到一个 client 发来的命令后会立即
 * 处理并 返回处理结果,但是当一个 client 在一个连接中发出 multi 命令有,这个连接会进入一个事务上下文,该连接后续的命令并不是立即执行,
 * 而是先放到一个队列中。当从此连接受到 exec 命令后,redis 会顺序的执行队列中的所有命令。并将所有命令的运行结果打包到一起返回给 client.
 * 然后此连接就 结束事务上下文。
 *
 *  eg:
 *      MULTI
 *      OK
 *      INCR foo
 *      QUEUED
 *      INCR bar
 *      QUEUED
 *      EXEC

 *      1) :1
 *      2) :1
 *
 */
public class Transaction extends MultiKeyPipelineBase {

  protected boolean inTransaction = true;

  protected Transaction() {
    // client will be set later in transaction block
  }

  public Transaction(final Client client) {
    this.client = client;
  }

  @Override
  protected Client getClient(String key) {
    return client;
  }

  @Override
  protected Client getClient(byte[] key) {
    return client;
  }

  /**
   * 执行事务，并获取事务执行所有响应数据结果
   * 
   * 事务和管道在执行时就为响应消息设置了消息构建器，并按命令的执行顺序将响应入队，当需要获取某条命令执行结果时，要在执行exec函数前
   * 用Response<T>接收，待事务执行后，可调用相应响应中的get方法即可获取执行结果，get方法在执行过程中会对服务器返回的数据进行自动构建。
   * 也可以获取事务执行后，用List集合接收所有响应结果。
   */
  public List<Object> exec() {
    client.exec();
    client.getAll(1); // Discard all but the last reply
    inTransaction = false;

    List<Object> unformatted = client.getObjectMultiBulkReply();
    if (unformatted == null) {
      return null;
    }
    List<Object> formatted = new ArrayList<Object>();
    for (Object o : unformatted) {
      try {
        formatted.add(generateResponse(o).get());
      } catch (JedisDataException e) {
        formatted.add(e);
      }
    }
    return formatted;
  }

  /**
   * 执行事务，并获取事务执行所有响应
   * 执行事务，放弃最后一个服务器响应（之前得到的是事务命令入队响应，最后一个是事务执行结果即多条批量回复信息）
   */
  public List<Response<?>> execGetResponse() {
    client.exec();
    client.getAll(1); // Discard all but the last reply
    inTransaction = false;

    List<Object> unformatted = client.getObjectMultiBulkReply();
    if (unformatted == null) {
      return null;
    }
    List<Response<?>> response = new ArrayList<Response<?>>();
    for (Object o : unformatted) {
      response.add(generateResponse(o));
    }
    return response;
  }

  public String discard() {
    client.discard();
    client.getAll(1); // Discard all but the last reply
    inTransaction = false;
    clean();
    return client.getStatusCodeReply();
  }

}