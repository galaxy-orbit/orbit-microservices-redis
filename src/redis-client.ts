import { ClientProxy, type ReadPacket, type WritePacket } from '@galaxy-stack/orbit-microservices';
import { RedisConnection, type RedisConnectionOptions } from './redis-connection';

export interface RedisClientOptions extends Partial<RedisConnectionOptions> {
  requestTimeout?: number;
  serializer?: {
    serialize: (value: any) => string;
    deserialize: (value: string) => any;
  };
}

interface RedisPendingRequest {
  callback: (packet: WritePacket) => void;
  timeout: ReturnType<typeof setTimeout>;
  startTime: number;
}

export class RedisClient extends ClientProxy {
  private readonly options: RedisClientOptions;
  private pubConnection: RedisConnection | null = null;
  private subConnection: RedisConnection | null = null;
  private redisPendingRequests: Map<string, RedisPendingRequest> = new Map();
  private subscribedReplyChannels: Set<string> = new Set();
  private serializer: { serialize: (v: any) => string; deserialize: (v: string) => any };
  private connectionStatus = false;

  constructor(options: RedisClientOptions = {}) {
    super();
    this.options = {
      host: options.host || 'localhost',
      port: options.port || 6379,
      retryAttempts: options.retryAttempts || 10,
      retryDelay: options.retryDelay || 1000,
      requestTimeout: options.requestTimeout || 30000,
      ...options,
    };
    
    this.serializer = options.serializer || {
      serialize: JSON.stringify,
      deserialize: JSON.parse,
    };
  }

  async connect(): Promise<void> {
    if (this.connectionStatus) return;

    this.pubConnection = new RedisConnection({
      host: this.options.host,
      port: this.options.port,
      password: this.options.password,
      db: this.options.db,
      retryAttempts: this.options.retryAttempts,
      retryDelay: this.options.retryDelay,
    });

    this.subConnection = new RedisConnection({
      host: this.options.host,
      port: this.options.port,
      password: this.options.password,
      db: this.options.db,
      retryAttempts: this.options.retryAttempts,
      retryDelay: this.options.retryDelay,
    });

    this.subConnection.onMessage((channel, message) => {
      this.handleReply(channel, message);
    });

    this.subConnection.onError((error) => {
      console.error('[RedisClient] Subscription error:', error.message);
    });

    this.subConnection.onClose(() => {
      this.connectionStatus = false;
    });

    this.subConnection.onReconnect(() => {
      console.log('[RedisClient] Reconnected');
    });

    this.pubConnection.onError((error) => {
      console.error('[RedisClient] Publisher error:', error.message);
    });

    await Promise.all([
      this.pubConnection.connect(),
      this.subConnection.connect(),
    ]);

    this.connectionStatus = true;
    console.log(`[RedisClient] Connected to redis://${this.options.host}:${this.options.port}`);
  }

  private handleReply(channel: string, payload: string): void {
    try {
      const response = this.serializer.deserialize(payload);
      const { id, response: result, error } = response;
      
      const pending = this.redisPendingRequests.get(id);
      if (!pending) {
        return;
      }
      
      this.redisPendingRequests.delete(id);
      clearTimeout(pending.timeout);
      
      if (error) {
        pending.callback({ err: error.message || 'Unknown error', response: null });
      } else {
        pending.callback({ response: result });
      }
    } catch (error) {
      console.error('[RedisClient] Error handling reply:', error);
    }
  }

  protected publish(packet: ReadPacket, callback: (packet: WritePacket) => void): () => void {
    const id = this.generateId();
    const requestChannel = this.getRequestChannel(packet.pattern);
    const replyChannel = this.getReplyChannel(packet.pattern, id);

    const doPublish = async () => {
      try {
        if (!this.subscribedReplyChannels.has(replyChannel)) {
          await this.subConnection!.subscribe(replyChannel);
          this.subscribedReplyChannels.add(replyChannel);
        }

        const timeout = setTimeout(() => {
          if (this.redisPendingRequests.has(id)) {
            this.redisPendingRequests.delete(id);
            callback({ err: `Request timeout after ${this.options.requestTimeout}ms`, response: null });
          }
        }, this.options.requestTimeout);

        const pending: RedisPendingRequest = { 
          callback, 
          timeout,
          startTime: performance.now(),
        };
        
        this.redisPendingRequests.set(id, pending);

        const message = this.serializer.serialize({
          pattern: packet.pattern,
          data: packet.data,
          id,
        });

        await this.pubConnection!.publish(requestChannel, message);
      } catch (error: any) {
        callback({ err: error.message || 'Publish failed', response: null });
      }
    };

    doPublish();

    return () => {
      const pending = this.redisPendingRequests.get(id);
      if (pending) {
        clearTimeout(pending.timeout);
        this.redisPendingRequests.delete(id);
      }
    };
  }

  protected async dispatchEvent(packet: ReadPacket): Promise<void> {
    await this.connect();
    
    const channel = this.getEventChannel(packet.pattern);
    const message = this.serializer.serialize({
      pattern: packet.pattern,
      data: packet.data,
    });
    
    await this.pubConnection!.publish(channel, message);
  }

  private getRequestChannel(pattern: string | object): string {
    const patternStr = typeof pattern === 'object' ? JSON.stringify(pattern) : pattern;
    return `orbit:request:${patternStr}`;
  }

  private getReplyChannel(pattern: string | object, id: string): string {
    const patternStr = typeof pattern === 'object' ? JSON.stringify(pattern) : pattern;
    return `orbit:reply:${patternStr}:${id}`;
  }

  private getEventChannel(pattern: string | object): string {
    const patternStr = typeof pattern === 'object' ? JSON.stringify(pattern) : pattern;
    return `orbit:event:${patternStr}`;
  }

  async close(): Promise<void> {
    for (const [id, pending] of this.redisPendingRequests) {
      clearTimeout(pending.timeout);
      pending.callback({ err: 'Client closed', response: null });
    }
    this.redisPendingRequests.clear();
    
    this.subscribedReplyChannels.clear();

    if (this.subConnection) {
      await this.subConnection.quit();
      this.subConnection = null;
    }

    if (this.pubConnection) {
      await this.pubConnection.quit();
      this.pubConnection = null;
    }

    this.connectionStatus = false;
    console.log('[RedisClient] Closed');
  }

  get connected(): boolean {
    return this.connectionStatus;
  }

  get pendingRequestCount(): number {
    return this.redisPendingRequests.size;
  }
}
