import { ClientOptions, Closeable } from '@nestjs/microservices';

import { RMQClient } from './rmq-client';

export class RMQClientFactory {
  /**
   * @param {ClientOptions} clientOptions
   *
   * @returns RmqClient | Closeable
   */
  public static create(clientOptions: ClientOptions): RMQClient & Closeable {
    const { options } = clientOptions;

    return new RMQClient(options);
  }
}
