package redis.clients.jedis;

import redis.clients.jedis.exceptions.JedisDataException;

import java.util.ArrayList;
import java.util.List;

/**
  * 客户端可以通过流水线， 在一次写入操作中发送多个命令：
  *     在发送新命令之前， 无须阅读前一个命令的回复。
  *     多个命令的回复会在最后一并返回。
*/
public class Pipeline extends MultiKeyPipelineBase {

  // 当前多响应消息构建器
  private MultiResponseBuilder currentMulti;

  /**
   * 多响应消息构建器，可用来存储流水线命令执行中所有的响应。
   */
  private class MultiResponseBuilder extends Builder<List<Object>> {
    private List<Response<?>> responses = new ArrayList<Response<?>>();

    // 在对管道式事务命令执行响应进行build时，会先对其依赖的当前多响应消息构建器进行build
    @Override
    public List<Object> build(Object data) {
      @SuppressWarnings("unchecked")
      List<Object> list = (List<Object>) data;
      List<Object> values = new ArrayList<Object>();

      if (list.size() != responses.size()) {
        throw new JedisDataException("Expected data size " + responses.size() + " but was "
            + list.size());
      }

      for (int i = 0; i < list.size(); i++) {
        Response<?> response = responses.get(i);
        // 绑定多响应消息构建器中各个响应的数据
        response.set(list.get(i));
        Object builtResponse;
        try {
          builtResponse = response.get();
        } catch (JedisDataException e) {
          builtResponse = e;
        }
        values.add(builtResponse);
      }
      return values;
    }

    // 为当前多个响应设置依赖
    public void setResponseDependency(Response<?> dependency) {
      for (Response<?> response : responses) {
        response.setDependency(dependency);
      }
    }

    public void addResponse(Response<?> response) {
      responses.add(response);
    }
  }

  /**
   * 当前多命令流水线如若开启，则将命令入队响应信息添加至响应队列中,将命令执行响应信息增加到当前多响应构建器中。
   * 在执行exec方法时，将当前多响应构建器也添加至响应队列中。
   */
  @Override
  protected <T> Response<T> getResponse(Builder<T> builder) {
    if (currentMulti != null) {
      super.getResponse(BuilderFactory.STRING); // Expected QUEUED

      Response<T> lr = new Response<T>(builder);
      currentMulti.addResponse(lr);
      return lr;
    } else {
      return super.getResponse(builder);
    }
  }

  public void setClient(Client client) {
    this.client = client;
  }

  @Override
  protected Client getClient(byte[] key) {
    return client;
  }

  @Override
  protected Client getClient(String key) {
    return client;
  }

  /**
   * Synchronize pipeline by reading all responses. This operation close the pipeline. In order to
   * get return values from pipelined commands, capture the different Response<?> of the commands
   * you execute.
   */
  // 获取流水线多命令的执行结果数据，并将其绑定到响应结果上
  public void sync() {
    if (getPipelinedResponseLength() > 0) {
      List<Object> unformatted = client.getAll();
      for (Object o : unformatted) {
        generateResponse(o);
      }
    }
  }

  /**
   * Synchronize pipeline by reading all responses. This operation close the pipeline. Whenever
   * possible try to avoid using this version and use Pipeline.sync() as it won't go through all the
   * responses and generate the right response type (usually it is a waste of time).
   * @return A list of all the responses in the order you executed them.
   */
  // 获取流水线多命令的执行结果数据，并将其绑定到响应结果上,继而调用各个响应的get方法自动构建所有响应信息，返回响应消息结果集
  public List<Object> syncAndReturnAll() {
    if (getPipelinedResponseLength() > 0) {
      List<Object> unformatted = client.getAll();
      List<Object> formatted = new ArrayList<Object>();

      for (Object o : unformatted) {
        try {
          formatted.add(generateResponse(o).get());
        } catch (JedisDataException e) {
          formatted.add(e);
        }
      }
      return formatted;
    } else {
      return java.util.Collections.<Object> emptyList();
    }
  }

  /**
   * 放弃流水线多命令（事务）执行，取消当前多响应消息构建器强引用
   * eg:
   *    cli: DISCARD
   *    server: OK
   */
  public Response<String> discard() {
    if (currentMulti == null) throw new JedisDataException("DISCARD without MULTI");

    client.discard();
    currentMulti = null;
    return getResponse(BuilderFactory.STRING);
  }

  /**
   * 执行流水线中的多命令（事务），得到当前多响应消息构建器的响应，并将该响应设置为它的依赖（便于对管道式事务执行结果进行build）
   * 
   * 管道式事务：
   * eg:
   *    cli: multi
   *         set foo 2
   *         get foo
   *         exec
   *    server: OK
   *            QUEUED
   *            QUEUED
   * 
   *            OK
   *            2
   */
  public Response<List<Object>> exec() {
    if (currentMulti == null) throw new JedisDataException("EXEC without MULTI");

    client.exec();
    Response<List<Object>> response = super.getResponse(currentMulti);
    currentMulti.setResponseDependency(response);
    currentMulti = null;
    return response;
  }

  /**
   * 开启一个流水线多命令（事务），并初始化一个多响应消息构建器
   * eg:
   *    cli: MULTI
        server: OK
   */
  public Response<String> multi() {
    if (currentMulti != null) throw new JedisDataException("MULTI calls can not be nested");

    client.multi();
    Response<String> response = getResponse(BuilderFactory.STRING); // Expecting
    // OK
    currentMulti = new MultiResponseBuilder();
    return response;
  }

}
