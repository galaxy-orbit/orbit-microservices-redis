# @galaxy-stack/orbit-microservices-redis

[![npm version](https://img.shields.io/npm/v/@galaxy-stack/orbit-microservices-redis.svg)](https://www.npmjs.com/package/@galaxy-stack/orbit-microservices-redis)
[![docs](https://img.shields.io/badge/docs-galaxy--orbit--framework.vercel.app-blue)](https://galaxy-orbit-framework.vercel.app)

Part of the [Orbit framework](https://github.com/galaxy-orbit/packages) — a NestJS-style backend framework for [Bun](https://bun.sh).

## Installation

```bash
bun add @galaxy-stack/orbit-microservices-redis
```

# @galaxy-stack/orbit-microservices-redis

> **Status**: ✅ Full Implementation - Complete RESP protocol với Bun native TCP

## Mô tả
Redis Pub/Sub transport implementation cho Orbit microservices với full RESP protocol support.

## Tính năng

- Complete RESP (Redis Serialization Protocol) parser/encoder
- Pub/Sub với request-response và event patterns
- Automatic reconnection với exponential backoff
- Connection pooling (separate pub/sub connections)
- Command timeout handling
- Error recovery và graceful shutdown
- JSON serialization với custom serializer support

## Cài đặt

```bash
bun add @galaxy-stack/orbit-microservices-redis
```

## Sử dụng

### Redis Server

```typescript
import { BunFactory } from '@galaxy-stack/orbit-core';
import '@galaxy-stack/orbit-microservices-redis';

const app = await BunFactory.createMicroservice(AppModule, {
  transport: 'REDIS',
  options: {
    host: 'localhost',
    port: 6379,
    password: 'your-password', // optional
    db: 0,                     // optional
    retryAttempts: 10,
    retryDelay: 1000,
  },
});

await app.listen();
// [RedisServer] Connected to Redis (PONG)
// [RedisServer] Listening on redis://localhost:6379
```

### Redis Client

```typescript
import { MicroservicesModule } from '@galaxy-stack/orbit-microservices';
import '@galaxy-stack/orbit-microservices-redis';

@Module({
  imports: [
    MicroservicesModule.register({
      name: 'REDIS_SERVICE',
      transport: 'REDIS',
      options: {
        host: 'localhost',
        port: 6379,
        requestTimeout: 30000,
      },
    }),
  ],
})
class ClientModule {}
```

### Message Handlers

```typescript
import { Controller, MessagePattern, EventPattern } from '@galaxy-stack/orbit-microservices';

@Controller()
class MathController {
  @MessagePattern('sum')
  sum(data: { a: number; b: number }): number {
    return data.a + data.b;
  }

  @EventPattern('user.created')
  handleUserCreated(data: { userId: number }): void {
    console.log('User created:', data.userId);
  }
}
```

### Client Usage

```typescript
@Injectable()
class OrderService {
  constructor(
    @Inject('REDIS_SERVICE') private client: ClientProxy
  ) {}

  async calculateTotal(items: any[]): Promise<number> {
    return this.client.send('calculate_total', { items });
  }

  notifyShipping(orderId: number): void {
    this.client.emit('order.shipped', { orderId });
  }
}
```

## Options

### RedisServerOptions

```typescript
interface RedisServerOptions {
  host?: string;           // Default: 'localhost'
  port?: number;           // Default: 6379
  password?: string;       // Redis password
  db?: number;             // Redis database number (0-15)
  retryAttempts?: number;  // Default: 10
  retryDelay?: number;     // Default: 1000ms
  connectTimeout?: number; // Default: 10000ms
  commandTimeout?: number; // Default: 30000ms
  wildcards?: boolean;     // Enable pattern subscription
  serializer?: {
    serialize: (value: any) => string;
    deserialize: (value: string) => any;
  };
}
```

### RedisClientOptions

```typescript
interface RedisClientOptions extends RedisServerOptions {
  requestTimeout?: number; // Default: 30000ms
}
```

## RESP Protocol

Full implementation of Redis Serialization Protocol:

```typescript
import { RespParser, RespEncoder } from '@galaxy-stack/orbit-microservices-redis';

// Parsing RESP responses
const parser = new RespParser();
parser.append(Buffer.from('+OK\r\n'));
const result = parser.parse(); // 'OK'

// Encoding commands
const command = RespEncoder.encodeCommand('SET', 'key', 'value');
// *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
```

### Supported RESP Types

| Type | Prefix | Example |
|------|--------|---------|
| Simple String | + | +OK\r\n |
| Error | - | -ERR message\r\n |
| Integer | : | :1000\r\n |
| Bulk String | $ | $5\r\nhello\r\n |
| Array | * | *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n |
| Null | $ | $-1\r\n |

## RedisConnection

Low-level Redis connection với event handlers:

```typescript
import { RedisConnection } from '@galaxy-stack/orbit-microservices-redis';

const conn = new RedisConnection({
  host: 'localhost',
  port: 6379,
});

conn.onMessage((channel, message) => {
  console.log(`Received on ${channel}: ${message}`);
});

conn.onError((error) => {
  console.error('Connection error:', error);
});

conn.onReconnect(() => {
  console.log('Reconnected to Redis');
});

await conn.connect();
await conn.subscribe('my-channel');
await conn.publish('my-channel', 'Hello!');
await conn.quit();
```

## Channel Naming

| Type | Pattern |
|------|---------|
| Request | orbit:request:{pattern} |
| Reply | orbit:reply:{pattern}:{id} |
| Event | orbit:event:{pattern} |

## Error Handling

```typescript
try {
  const result = await client.send('divide', { a: 10, b: 0 });
} catch (error) {
  console.error(error.message); // 'Division by zero'
}
```

## Reconnection

Automatic reconnection với exponential backoff:

- Retry attempts: configurable (default: 10)
- Backoff: delay * 2^attempt
- Automatic resubscription after reconnect

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Application                          │
├─────────────────────────────────────────────────────────┤
│     RedisServer          │        RedisClient          │
├─────────────────────────────────────────────────────────┤
│              RedisConnection (Pub/Sub)                  │
├─────────────────────────────────────────────────────────┤
│              RespParser / RespEncoder                   │
├─────────────────────────────────────────────────────────┤
│                   Bun.connect()                         │
└─────────────────────────────────────────────────────────┘
```
