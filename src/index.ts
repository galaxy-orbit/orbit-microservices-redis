import { registerTransport, Transport } from '@galaxy-stack/orbit-microservices';
import { RedisServer, type RedisServerOptions } from './redis-server';
import { RedisClient, type RedisClientOptions } from './redis-client';

registerTransport(Transport.REDIS, RedisServer, RedisClient);

export { RedisServer, type RedisServerOptions } from './redis-server';
export { RedisClient, type RedisClientOptions } from './redis-client';
export { RedisConnection, type RedisConnectionOptions } from './redis-connection';
export { 
  RespParser, 
  RespEncoder, 
  parsePubSubMessage,
  type RespValue,
  type PubSubMessage,
} from './resp-protocol';
