import { Server, type TransportOptions, type OutgoingMessage } from '@galaxy-stack/orbit-microservices';
import { RedisConnection, type RedisConnectionOptions } from './redis-connection';

export interface RedisServerOptions extends TransportOptions, Partial<RedisConnectionOptions> {
  wildcards?: boolean;
  serializer?: {
    serialize: (value: any) => string;
    deserialize: (value: string) => any;
  };
}

interface ChannelInfo {
  pattern: string;
  isEvent: boolean;
}

export class RedisServer extends Server {
  private readonly options: RedisServerOptions;
  private channelMap: Map<string, ChannelInfo> = new Map();
  private pubConnection: RedisConnection | null = null;
  private subConnection: RedisConnection | null = null;
  private isListening = false;
  private serializer: { serialize: (v: any) => string; deserialize: (v: string) => any };

  constructor(options: RedisServerOptions = {}) {
    super();
    this.options = {
      host: options.host || 'localhost',
      port: options.port || 6379,
      retryAttempts: options.retryAttempts || 10,
      retryDelay: options.retryDelay || 1000,
      wildcards: options.wildcards ?? false,
      ...options,
    };
    
    this.serializer = options.serializer || {
      serialize: JSON.stringify,
      deserialize: JSON.parse,
    };
  }

  async listen(callback?: () => void): Promise<void> {
    try {
      await this.connect();
      this.isListening = true;
      await this.setupSubscriptions();
      console.log(`[RedisServer] Listening on redis://${this.options.host}:${this.options.port}`);
      callback?.();
    } catch (error) {
      console.error('[RedisServer] Failed to start:', error);
      throw error;
    }
  }

  private async connect(): Promise<void> {
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
      this.processIncomingMessage(channel, message);
    });

    this.subConnection.onError((error) => {
      console.error('[RedisServer] Subscription error:', error.message);
    });

    this.subConnection.onReconnect(() => {
      console.log('[RedisServer] Reconnected, resubscribing...');
    });

    this.pubConnection.onError((error) => {
      console.error('[RedisServer] Publisher error:', error.message);
    });

    await Promise.all([
      this.pubConnection.connect(),
      this.subConnection.connect(),
    ]);

    const pong = await this.pubConnection.ping();
    console.log(`[RedisServer] Connected to Redis (${pong})`);
  }

  private async setupSubscriptions(): Promise<void> {
    const handlers = this.getHandlers();
    
    for (const [pattern] of handlers) {
      const requestChannel = this.getRequestChannel(pattern);
      await this.subConnection!.subscribe(requestChannel);
      this.channelMap.set(requestChannel, { pattern, isEvent: false });
      console.log(`[RedisServer] Subscribed to pattern: ${pattern}`);

      const eventChannel = this.getEventChannel(pattern);
      await this.subConnection!.subscribe(eventChannel);
      this.channelMap.set(eventChannel, { pattern, isEvent: true });
    }
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

  private async processIncomingMessage(channel: string, payload: string): Promise<void> {
    try {
      const message = this.serializer.deserialize(payload);
      const { pattern, data, id } = message;
      
      const channelInfo = this.channelMap.get(channel);
      if (!channelInfo) {
        console.warn(`[RedisServer] No handler for channel: ${channel}`);
        return;
      }

      if (channelInfo.isEvent || !id) {
        await this.handleMessage(channelInfo.pattern, data);
      } else {
        const startTime = performance.now();
        const replyChannel = this.getReplyChannel(pattern, id);
        
        const respond = async (response: OutgoingMessage) => {
          const duration = Math.round(performance.now() - startTime);
          
          if (response.err) {
            await this.pubConnection!.publish(
              replyChannel,
              this.serializer.serialize({
                error: { message: response.err },
                id,
                duration,
              })
            );
          } else {
            await this.pubConnection!.publish(
              replyChannel,
              this.serializer.serialize({
                response: response.response,
                id,
                duration,
              })
            );
          }
        };
        
        await this.handleMessage(channelInfo.pattern, data, respond);
      }
    } catch (error) {
      console.error('[RedisServer] Error processing message:', error);
    }
  }

  async close(): Promise<void> {
    this.isListening = false;
    
    if (this.subConnection) {
      for (const channel of this.channelMap.keys()) {
        try {
          await this.subConnection.unsubscribe(channel);
        } catch {
        }
      }
      await this.subConnection.quit();
      this.subConnection = null;
    }

    if (this.pubConnection) {
      await this.pubConnection.quit();
      this.pubConnection = null;
    }

    this.channelMap.clear();
    console.log('[RedisServer] Closed');
  }

  get listening(): boolean {
    return this.isListening;
  }

  get subscribedPatterns(): string[] {
    return Array.from(this.channelMap.values()).map(c => c.pattern);
  }
}
