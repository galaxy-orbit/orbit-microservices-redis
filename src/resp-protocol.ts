export type RespValue = 
  | string 
  | number 
  | null 
  | RespValue[] 
  | Error
  | Buffer;

export class RespParser {
  private buffer: Buffer = Buffer.alloc(0);
  private offset: number = 0;

  append(data: Buffer): void {
    this.buffer = Buffer.concat([this.buffer.subarray(this.offset), data]);
    this.offset = 0;
  }

  parse(): RespValue | null {
    if (this.buffer.length === 0) return null;

    try {
      const result = this.parseValue();
      this.buffer = this.buffer.subarray(this.offset);
      this.offset = 0;
      return result;
    } catch (e) {
      if (e instanceof IncompleteDataError) {
        // rewind so a later append() re-parses from the message start
        this.offset = 0;
        return null;
      }
      throw e;
    }
  }

  parseAll(): RespValue[] {
    const results: RespValue[] = [];
    let result: RespValue | null;
    
    while ((result = this.parse()) !== null) {
      results.push(result);
    }
    
    return results;
  }

  private parseValue(): RespValue {
    if (this.offset >= this.buffer.length) {
      throw new IncompleteDataError();
    }

    const type = String.fromCharCode(this.buffer[this.offset]);
    this.offset++;

    switch (type) {
      case '+': return this.parseSimpleString();
      case '-': return this.parseError();
      case ':': return this.parseInteger();
      case '$': return this.parseBulkString();
      case '*': return this.parseArray();
      default:
        throw new Error(`Unknown RESP type: ${type}`);
    }
  }

  private parseSimpleString(): string {
    const line = this.readLine();
    return line;
  }

  private parseError(): Error {
    const line = this.readLine();
    return new Error(line);
  }

  private parseInteger(): number {
    const line = this.readLine();
    return parseInt(line, 10);
  }

  private parseBulkString(): string | null {
    const lengthLine = this.readLine();
    const length = parseInt(lengthLine, 10);

    if (length === -1) {
      return null;
    }

    if (this.offset + length + 2 > this.buffer.length) {
      throw new IncompleteDataError();
    }

    const str = this.buffer.subarray(this.offset, this.offset + length).toString();
    this.offset += length + 2;
    return str;
  }

  private parseArray(): RespValue[] | null {
    const lengthLine = this.readLine();
    const length = parseInt(lengthLine, 10);

    if (length === -1) {
      return null;
    }

    const array: RespValue[] = [];
    for (let i = 0; i < length; i++) {
      array.push(this.parseValue());
    }
    return array;
  }

  private readLine(): string {
    const start = this.offset;
    while (this.offset < this.buffer.length) {
      if (this.buffer[this.offset] === 0x0d && 
          this.offset + 1 < this.buffer.length && 
          this.buffer[this.offset + 1] === 0x0a) {
        const line = this.buffer.subarray(start, this.offset).toString();
        this.offset += 2;
        return line;
      }
      this.offset++;
    }
    this.offset = start;
    throw new IncompleteDataError();
  }

  reset(): void {
    this.buffer = Buffer.alloc(0);
    this.offset = 0;
  }
}

class IncompleteDataError extends Error {
  constructor() {
    super('Incomplete RESP data');
    this.name = 'IncompleteDataError';
  }
}

export class RespEncoder {
  static encode(args: (string | number | Buffer)[]): Buffer {
    let command = `*${args.length}\r\n`;
    
    for (const arg of args) {
      if (typeof arg === 'string') {
        const bytes = Buffer.byteLength(arg);
        command += `$${bytes}\r\n${arg}\r\n`;
      } else if (typeof arg === 'number') {
        const str = arg.toString();
        const bytes = Buffer.byteLength(str);
        command += `$${bytes}\r\n${str}\r\n`;
      } else if (Buffer.isBuffer(arg)) {
        command += `$${arg.length}\r\n`;
        return Buffer.concat([
          Buffer.from(command),
          arg,
          Buffer.from('\r\n'),
        ]);
      }
    }
    
    return Buffer.from(command);
  }

  static encodeCommand(...args: (string | number)[]): Buffer {
    return this.encode(args);
  }

  static encodeSimpleString(str: string): string {
    return `+${str}\r\n`;
  }

  static encodeError(message: string): string {
    return `-${message}\r\n`;
  }

  static encodeInteger(num: number): string {
    return `:${num}\r\n`;
  }

  static encodeBulkString(str: string | null): string {
    if (str === null) {
      return '$-1\r\n';
    }
    const bytes = Buffer.byteLength(str);
    return `$${bytes}\r\n${str}\r\n`;
  }

  static encodeArray(arr: (string | number | null)[] | null): string {
    if (arr === null) {
      return '*-1\r\n';
    }
    
    let result = `*${arr.length}\r\n`;
    for (const item of arr) {
      if (item === null) {
        result += '$-1\r\n';
      } else if (typeof item === 'number') {
        result += `:${item}\r\n`;
      } else {
        result += this.encodeBulkString(item);
      }
    }
    return result;
  }
}

export interface PubSubMessage {
  type: 'message' | 'pmessage' | 'subscribe' | 'unsubscribe' | 'psubscribe' | 'punsubscribe';
  channel: string;
  pattern?: string;
  message?: string;
  count?: number;
}

export function parsePubSubMessage(data: RespValue[]): PubSubMessage | null {
  if (!Array.isArray(data) || data.length < 2) {
    return null;
  }

  const type = data[0] as string;
  
  switch (type) {
    case 'message':
      return {
        type: 'message',
        channel: data[1] as string,
        message: data[2] as string,
      };
    
    case 'pmessage':
      return {
        type: 'pmessage',
        pattern: data[1] as string,
        channel: data[2] as string,
        message: data[3] as string,
      };
    
    case 'subscribe':
    case 'unsubscribe':
    case 'psubscribe':
    case 'punsubscribe':
      return {
        type: type as PubSubMessage['type'],
        channel: data[1] as string,
        count: data[2] as number,
      };
    
    default:
      return null;
  }
}
