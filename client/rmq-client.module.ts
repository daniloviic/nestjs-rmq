import { DynamicModule, Module, OnApplicationShutdown } from "@nestjs/common";
import {
  ClientProxy,
  ClientsModuleOptions,
  ClientProviderOptions,
  ClientsModuleAsyncOptions,
  Closeable,
} from "@nestjs/microservices";

// Factory
import { RMQClientFactory } from "./rmq-client-factory";

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
   * Registers the module asynchronously.
   * @param options - Module options.
   * @returns The registered module.
   */
  static async registerAsync(
    options: ClientsModuleAsyncOptions
  ): Promise<DynamicModule> {
    let imports = [];
    const clients = [];

    for (const item of options || []) {
      imports = [...imports, ...item.imports];

      // Create the options for the provider.
      const providerOptions = `${String(item.name)}_OPTIONS`;
      clients.push({
        provide: providerOptions,
        useFactory: item.useFactory,
        inject: item.inject,
      });

      // Create the provider based on the options.
      clients.push({
        provide: item.name,
        useFactory: (options: ClientProviderOptions) =>
          this.assignOnAppShutdownHook(RMQClientFactory.create(options)),
        inject: [providerOptions],
      });
    }

    return {
      module: RMQClientModule,
      imports: imports,
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
