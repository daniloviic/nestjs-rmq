import { RmqUrl } from '@nestjs/microservices/external/rmq-url.interface';
import { Deserializer, Serializer, Transport } from '@nestjs/microservices';

// Enums
import { ExchangeType } from '../static/rmq.static';

export type RmqOptions = {
  transport?: Transport.RMQ;
  options?: {
    urls?: string[] | RmqUrl[];
    queue?: string;
    exchangeType?: ExchangeType;
    prefetchCount?: number;
    isGlobalPrefetchCount?: boolean;
    queueOptions?: any;
    socketOptions?: any;
    exchangeOptions?: any;
    noAck?: boolean;
    serializer?: Serializer;
    deserializer?: Deserializer;
    replyQueue?: string;
    persistent?: boolean;
  };
};
