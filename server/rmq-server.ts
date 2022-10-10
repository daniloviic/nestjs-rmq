import { CustomTransportStrategy, ServerRMQ } from '@nestjs/microservices';
import { RQM_DEFAULT_NOACK } from '@nestjs/microservices/constants';
import { Channel, Replies } from 'amqplib';

// Contracts
import { RmqOptions } from '../contracts/rmq.types';

// Static
import { ExchangeType } from '../static/rmq.static';

/**
 * Used to enable a PUB/SUB system with Nest.js and RabbitMQ.
 */
export class RMQServer extends ServerRMQ implements CustomTransportStrategy {
  /**
   * Supported exchange types: DIRECT & FANOUT
   */
  protected readonly exchangeType: ExchangeType;

  protected readonly exchangeOptions: any;

  /**
   * @param {RmqOptions} options
   */
  constructor(protected readonly options: RmqOptions['options']) {
    super(options);

    this.exchangeOptions = options?.exchangeOptions || {};
    this.exchangeType = options?.exchangeType || ExchangeType.DIRECT;
  }

  /**
   * Overrides ServerRMQ's setupChannel method.
   * This method is called upon Server start.
   * @param channel - AMQP Channel.
   * @param {function} callback - A callback function to wire everything up with Nest.
   */
  public async setupChannel(channel: Channel, callback: any): Promise<void> {
    // Acknowledgement mode.
    const noAck = this.options?.noAck || RQM_DEFAULT_NOACK;
    let queue: Replies.AssertQueue;

    // Setup a direct exchange.
    if (this.exchangeType === ExchangeType.DIRECT) {
      queue = await this.setupDirectExchange(channel);
    }
    // Setup a fanout exchange
    else {
      queue = await this.setupFanoutExchange(channel);
    }

    // Fair dispatch.
    // In a situation with two workers, when all odd messages are heavy and even messages are light, one worker will be constantly busy and the other one will do hardly any work.
    // In order to defeat that we can use the prefetch method with the value of X. This tells RabbitMQ not to give more than X messages to a worker at a time.
    // Or, in other words, don't dispatch a new message to a worker until it has processed and acknowledged the previous X messages. Instead, it will dispatch it to the next worker that is not still busy.
    await channel.prefetch(this.prefetchCount, this.isGlobalPrefetchCount);

    // Consume the queue.
    channel.consume(
      queue.queue,
      // Pass the message to the built in server to handle it.
      (msg: Record<string, any> | null) => {
        if (!msg) {
          return;
        }
        this.handleMessage(msg, channel);
      },
      {
        noAck,
      },
    );
    callback();
  }

  /**
   * Sets up a Direct exchange.
   * @param channel - AMQP Channel.
   * @returns Asserted queue.
   */
  private async setupDirectExchange(
    channel: Channel,
  ): Promise<Replies.AssertQueue> {
    return channel.assertQueue(this.queue, this.queueOptions);
  }

  /**
   * Sets up a fanout exchange.
   * @param channel - AMQP Channel.
   * @returns Asserted queue.
   */
  private async setupFanoutExchange(
    channel: Channel,
  ): Promise<Replies.AssertQueue> {
    // Assert exchange's existance.
    await channel.assertExchange(
      this.queue,
      this.exchangeType,
      this.exchangeOptions,
    );

    // We want to hear about all messages, not just a subset of them.
    // We're also interested only in currently flowing messages not in the old ones.

    // Firstly, whenever we connect to Rabbit we need a fresh, empty queue.
    const queue = await channel.assertQueue(
      // When we supply queue name as an empty string, we create a non-durable queue with a generated name. For example it may look like amq.gen-JzTY20BRgKO-HjmUJj0wLg.
      '',
      // Secondly, once we disconnect the consumer the queue should be automatically deleted.
      // When the connection that declared the queue closes, the queue will be deleted because it is declared as exclusive.
      {
        exclusive: true,
      },
    );

    // We've already created a fanout exchange and a queue.
    // Now we need to tell the exchange to send messages to our queue.
    // That relationship between exchange and a queue is called a binding.
    await channel.bindQueue(queue.queue, this.queue, '');

    return queue;
  }
}
