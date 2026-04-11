export type DebugLevel = 'trace' | 'debug' | 'info' | 'warn' | 'error';

type LoggerFunction = (...messages: unknown[]) => void;

interface Logger {
  trace: LoggerFunction;
  debug: LoggerFunction;
  info: LoggerFunction;
  warn: LoggerFunction;
  error: LoggerFunction;
  setLevel: (level: DebugLevel) => void;
}

let currentLevel: DebugLevel = process.env.NODE_ENV === 'development' ? 'debug' : 'info';

const isServer = typeof window === 'undefined';
const supportsColor = !isServer;

export const logger: Logger = {
  trace: (...messages: unknown[]) => log('trace', undefined, messages),
  debug: (...messages: unknown[]) => log('debug', undefined, messages),
  info: (...messages: unknown[]) => log('info', undefined, messages),
  warn: (...messages: unknown[]) => log('warn', undefined, messages),
  error: (...messages: unknown[]) => log('error', undefined, messages),
  setLevel,
};

export function createScopedLogger(scope: string): Logger {
  return {
    trace: (...messages: unknown[]) => log('trace', scope, messages),
    debug: (...messages: unknown[]) => log('debug', scope, messages),
    info: (...messages: unknown[]) => log('info', scope, messages),
    warn: (...messages: unknown[]) => log('warn', scope, messages),
    error: (...messages: unknown[]) => log('error', scope, messages),
    setLevel,
  };
}

function stringifyLogMessage(message: unknown): string {
  if (typeof message === 'string') {
    return message;
  }

  if (message instanceof Error) {
    return message.stack ?? message.message;
  }

  try {
    return JSON.stringify(message) ?? String(message);
  } catch {
    return String(message);
  }
}

function setLevel(level: DebugLevel) {
  if ((level === 'trace' || level === 'debug') && process.env.NODE_ENV === 'production') {
    return;
  }

  currentLevel = level;
}

function log(level: DebugLevel, scope: string | undefined, messages: unknown[]) {
  const levelOrder: DebugLevel[] = ['trace', 'debug', 'info', 'warn', 'error'];

  if (levelOrder.indexOf(level) < levelOrder.indexOf(currentLevel)) {
    return;
  }

  const allMessages = messages.map(stringifyLogMessage).reduce((acc, current) => {
    if (acc.endsWith('\n')) {
      return acc + current;
    }

    if (!acc) {
      return current;
    }

    return `${acc} ${current}`;
  }, '');

  if (!supportsColor) {
    console.log(`[${level.toUpperCase()}]`, scope ? `[${scope}]` : '', allMessages);

    return;
  }

  const labelBackgroundColor = getColorForLevel(level);
  const labelTextColor = level === 'warn' ? 'black' : 'white';

  const labelStyles = getLabelStyles(labelBackgroundColor, labelTextColor);
  const scopeStyles = getLabelStyles('#77828D', 'white');

  const styles = [labelStyles];

  if (typeof scope === 'string') {
    styles.push('', scopeStyles);
  }

  console.log(`%c${level.toUpperCase()}${scope ? `%c %c${scope}` : ''}`, ...styles, allMessages);
}

function getLabelStyles(color: string, textColor: string) {
  return `background-color: ${color}; color: white; border: 4px solid ${color}; color: ${textColor};`;
}

function getColorForLevel(level: DebugLevel): string {
  switch (level) {
    case 'trace':
    case 'debug': {
      return '#77828D';
    }
    case 'info': {
      return '#1389FD';
    }
    case 'warn': {
      return '#FFDB6C';
    }
    case 'error': {
      return '#EE4744';
    }
    default: {
      return 'black';
    }
  }
}

export const renderLogger = createScopedLogger('Render');
