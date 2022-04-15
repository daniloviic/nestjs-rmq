import { Injectable } from '@nestjs/common';
import { randomStringGenerator } from '@nestjs/common/utils/random-string-generator.util';
import { ClientRMQ, ReadPacket, WritePacket } from '@nestjs/microservices';
import {
  RQM_DEFAULT_IS_GLOBAL_PREFETCH_COUNT,
  RQM_DEFAULT_PREFETCH_COUNT,
} from '@nestjs/microservices/constants';
import { EventEmitter } from 'events';
import { Channel } from 'amqplib';

// Static
import { ExchangeType } from '../static/rmq.static';

/**
 * Used to enable a PUB/SUB system with Nest.js and RabbitMQ.
 */
@Injectable()
export class RMQClient extends ClientRMQ {
  /**
   * Supported exchange types: DIRECT & FANOUT
   */
  protected readonly exchangeType: ExchangeType;

  protected readonly exchangeOptions: any;

  /**
   * @param {RmqOptions[options]} options
   */
  constructor(protected readonly options) {
    super(options);

    this.exchangeOptions =
      this.getOptionsProp(options, 'exchangeOptions') || {};
    this.exchangeType =
      this.getOptionsProp(options, 'exchangeType') || ExchangeType.DIRECT;
  }

  /**
   * Overrides ClientRMQ's setupChannel method.
   * This method is called upon Client start.
   * @param channel - AMQP Channel.
   * @param resolve - A resolve function to wire everything up with Nest.
   */
  public async setupChannel(channel: Channel, resolve: any): Promise<any> {
    // Setup a direct exchange.
    if (this.exchangeType === ExchangeType.DIRECT) {
      await this.setupDirectExchange(channel);
    }
    // Setup a fanout exchange
    else if (this.exchangeType === ExchangeType.FANOUT) {
      await this.setupFanoutExchange(channel);
    }

    // Fair dispatch.
    // In a situation with two workers, when all odd messages are heavy and even messages are light, one worker will be constantly busy and the other one will do hardly any work.
    // In order to defeat that we can use the prefetch method with the value of X. This tells RabbitMQ not to give more than X messages to a worker at a time.
    // Or, in other words, don't dispatch a new message to a worker until it has processed and acknowledged the previous X messages. Instead, it will dispatch it to the next worker that is not still busy.
    const prefetchCount =
      this.getOptionsProp(this.options, 'prefetchCount') ||
      RQM_DEFAULT_PREFETCH_COUNT;
    const isGlobalPrefetchCount =
      this.getOptionsProp(this.options, 'isGlobalPrefetchCount') ||
      RQM_DEFAULT_IS_GLOBAL_PREFETCH_COUNT;
    await channel.prefetch(prefetchCount, isGlobalPrefetchCount);

    this.responseEmitter = new EventEmitter();
    this.responseEmitter.setMaxListeners(0);

    // Consume the channel.
    await this.consumeChannel(channel);
    resolve();
  }

  /**
   * Sets up a direct exchange.
   * @param channel - AMQP Channel.
   */
  private async setupDirectExchange(channel: Channel): Promise<void> {
    await channel.assertQueue(this.queue, this.queueOptions);
  }

  /**
   * Sets up a fanout exchange.
   * @param channel - AMQP Channel.
   */
  private async setupFanoutExchange(channel: Channel): Promise<void> {
    // Assert exchange's existance.
    await channel.assertExchange(
      this.queue,
      this.exchangeType,
      this.exchangeOptions,
    );
  }

  /**
   * Request-response communication.
   * @param message - Message to publish.
   * @param callback - A callback function to wire everything up with Nest.
   * @returns {function}
   */
  protected publish(
    message: ReadPacket,
    callback: (packet: WritePacket) => any,
  ): any {
    try {
      const correlationId = randomStringGenerator();
      const listener = ({ content }: { content: any }) =>
        this.handleMessage(JSON.parse(content.toString()), callback);

      Object.assign(message, { id: correlationId });

      const serializedPacket = this.serializer.serialize(message);
      const options = serializedPacket.options;
      delete serializedPacket.options;

      this.responseEmitter.on(correlationId, listener);

      // Publish to a direct exchange.
      if (this.exchangeType === ExchangeType.DIRECT) {
        this.publishToDirectExchange(serializedPacket, options, correlationId);
      }
      // Publish to a fanout exchange
      else if (this.exchangeType === ExchangeType.FANOUT) {
        this.publishToFanoutExchange(serializedPacket, options, correlationId);
      }
      return () => this.responseEmitter.removeListener(correlationId, listener);
    } catch (err) {
      callback({ err });
    }
  }

  /**
   * Publish to a direct exchange.
   * @param serializedPacket - Serialized message to publish.
   * @param options - Options object.
   * @param correlationId - Id.
   * @returns Boolean
   */
  private publishToDirectExchange(
    serializedPacket: any,
    options: any,
    correlationId: any,
  ): boolean {
    return this.channel.sendToQueue(
      this.queue,
      Buffer.from(JSON.stringify(serializedPacket)),
      Object.assign(
        Object.assign(
          { replyTo: this.replyQueue, persistent: this.persistent },
          options,
        ),
        {
          headers: this.mergeHeaders(
            options === null || options === void 0 ? void 0 : options.headers,
          ),
          correlationId,
        },
      ),
    );
  }

  /**
   * Publish to a fanout exchange.
   * @param serializedPacket - Serialized message to publish.
   * @param options - Options object.
   * @param correlationId - Id.
   * @returns Boolean
   */
  private publishToFanoutExchange(
    serializedPacket: any,
    options: any,
    correlationId: any,
  ): boolean {
    return this.channel.publish(
      this.queue,
      // The empty string as second parameter means that we don't want to send the message to any specific queue.
      // We want only to publish it to our exchange.
      '',
      Buffer.from(JSON.stringify(serializedPacket)),
      Object.assign(
        Object.assign(
          { replyTo: this.replyQueue, persistent: this.persistent },
          options,
        ),
        {
          headers: this.mergeHeaders(
            options === null || options === void 0 ? void 0 : options.headers,
          ),
          correlationId,
        },
      ),
    );
  }

  /**
   * Event-based communication
   * @param {ReadPacket} packet - Message to dipatch.
   * @protected
   *
   * @returns Promise<any>
   */
  protected dispatchEvent(packet: ReadPacket): Promise<any> {
    const serializedPacket = this.serializer.serialize(packet);
    const options = serializedPacket.options;
    delete serializedPacket.options;

    // Dispatch to a direct exchange.
    if (this.exchangeType === ExchangeType.DIRECT) {
      return this.dispatchToDirectExchange(serializedPacket, options);
    }
    // Dispatch to a fanout exchange
    else if (this.exchangeType === ExchangeType.FANOUT) {
      return this.dispatchToFanoutExchange(serializedPacket, options);
    }
  }

  /**
   * Publish to a direct exchange.
   * @param serializedPacket - Serialized message to publish.
   * @param options - Options object.
   * @returns Boolean
   */
  private async dispatchToDirectExchange(
    serializedPacket: any,
    options: any,
  ): Promise<any> {
    return new Promise<void>((resolve, reject) =>
      this.channel.sendToQueue(
        this.queue,
        Buffer.from(JSON.stringify(serializedPacket)),
        Object.assign(Object.assign({ persistent: this.persistent }, options), {
          headers: this.mergeHeaders(
            options === null || options === void 0 ? void 0 : options.headers,
          ),
        }),
        (err) => (err ? reject(err) : resolve()),
      ),
    );
  }

  /**
   * Publish to a fanout exchange.
   * @param serializedPacket - Serialized message to publish.
   * @param options - Options object.
   * @returns Boolean
   */
  private dispatchToFanoutExchange(
    serializedPacket: any,
    options: any,
  ): Promise<any> {
    return new Promise<void>((resolve, reject) => {
      this.channel.publish(
        this.queue,
        // The empty string as second parameter means that we don't want to send the message to any specific queue.
        // We want only to publish it to our exchange.
        '',
        Buffer.from(JSON.stringify(serializedPacket)),
        Object.assign(Object.assign({ persistent: this.persistent }, options), {
          headers: this.mergeHeaders(
            options === null || options === void 0 ? void 0 : options.headers,
          ),
        }),
        (err) => (err ? reject(err) : resolve()),
      );
    });
  }
}
