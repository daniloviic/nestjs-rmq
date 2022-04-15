import { DynamicModule, Module, OnApplicationShutdown } from '@nestjs/common';
import {
  ClientProxy,
  ClientsModuleOptions,
  ClientProviderOptions,
  Closeable,
} from '@nestjs/microservices';
import { RMQClientFactory } from './rmq-client-factory';

@Module({})
export class RMQClientModule {
  /**
   * Registers the module.
   * @param options - Module options.
   * @returns The registered module.
   */
  static register(options: ClientsModuleOptions): DynamicModule {
    const clients = (options || []).map((item: ClientProviderOptions) => ({
      provide: item.name,
      useValue: this.assignOnAppShutdownHook(RMQClientFactory.create(item)),
    }));

    return {
      module: RMQClientModule,
      providers: clients,
      exports: clients,
    };
  }

  /**
   * @param {ClientProxy | Closeable} client
   * @private
   */
  private static assignOnAppShutdownHook(client: ClientProxy & Closeable) {
    (client as unknown as OnApplicationShutdown).onApplicationShutdown =
      client.close;

    return client;
  }
}
