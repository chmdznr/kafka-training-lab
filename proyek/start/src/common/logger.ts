import type { LoggerService } from '@nestjs/common';

type LogLevel = 'info' | 'warn' | 'error' | 'debug' | 'verbose' | 'fatal';

/**
 * JSON logs, one object per line, so `grep <correlationId>` across service
 * logs returns the whole story of one order.
 */
export class JsonLogger implements LoggerService {
  constructor(private readonly service: string) {}

  log(message: unknown, context?: string): void {
    this.write('info', message, context);
  }

  warn(message: unknown, context?: string): void {
    this.write('warn', message, context);
  }

  error(message: unknown, stack?: string, context?: string): void {
    this.write('error', message, context, stack);
  }

  debug(message: unknown, context?: string): void {
    this.write('debug', message, context);
  }

  verbose(message: unknown, context?: string): void {
    this.write('verbose', message, context);
  }

  fatal(message: unknown, context?: string): void {
    this.write('fatal', message, context);
  }

  private write(
    level: LogLevel,
    message: unknown,
    context?: string,
    stack?: string,
  ): void {
    const entry: Record<string, unknown> = {
      ts: new Date().toISOString(),
      level,
      service: this.service,
    };
    if (context) {
      entry.context = context;
    }
    if (message instanceof Error) {
      entry.message = message.message;
      entry.stack = message.stack;
    } else if (message !== null && typeof message === 'object') {
      Object.assign(entry, message);
    } else {
      entry.message = String(message);
    }
    if (stack) {
      entry.stack = stack;
    }
    process.stdout.write(`${JSON.stringify(entry)}\n`);
  }
}
