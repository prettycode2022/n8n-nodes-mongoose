import mongoose from 'mongoose';

export interface ConnectionTestResult {
  isValid: boolean;
  error?: string;
  details?: {
    protocol: string;
    hosts: string[];
    database?: string;
    options?: Record<string, any>;
  };
}

export function parseMongoDBConnectionString(connectionString: string): ConnectionTestResult {
  try {
    // Basic URL validation
    if (!connectionString || typeof connectionString !== 'string') {
      return {
        isValid: false,
        error: 'Connection string is required and must be a string',
      };
    }

    // Check if it starts with mongodb:// or mongodb+srv://
    if (!connectionString.startsWith('mongodb://') && !connectionString.startsWith('mongodb+srv://')) {
      return {
        isValid: false,
        error: 'Connection string must start with mongodb:// or mongodb+srv://',
      };
    }

    // Parse MongoDB connection string manually for replica sets
    const protocol = connectionString.startsWith('mongodb+srv://') ? 'mongodb+srv' : 'mongodb';

    // Remove protocol
    let remaining = connectionString.replace(/^mongodb(\+srv)?:\/\//, '');

    // Extract auth info if present
    let authInfo = '';
    let username = '';
    let password = '';

    if (remaining.includes('@')) {
      const authSplit = remaining.split('@');
      authInfo = authSplit[0] || '';
      remaining = authSplit.slice(1).join('@');

      if (authInfo.includes(':')) {
        const authParts = authInfo.split(':');
        username = authParts[0] || '';
        password = authParts[1] || '';
      } else {
        username = authInfo;
      }
    }

    // Extract database and query string
    let database = '';
    let queryString = '';

    if (remaining.includes('?')) {
      const querySplit = remaining.split('?');
      remaining = querySplit[0] || '';
      queryString = querySplit.slice(1).join('?');
    }

    if (remaining.includes('/')) {
      const dbSplit = remaining.split('/');
      remaining = dbSplit[0] || '';
      database = dbSplit.slice(1).join('/');
    }

    // Parse hosts (this is where the issue was)
    const hosts: string[] = [];
    if (remaining) {
      const hostParts = remaining.split(',');
      hostParts.forEach(host => {
        if (host.trim()) {
          hosts.push(host.trim());
        }
      });
    }

    // Parse query parameters
    const options: Record<string, any> = {};
    if (queryString) {
      const params = new URLSearchParams(queryString);
      params.forEach((value, key) => {
        options[key] = value;
      });
    }

    return {
      isValid: true,
      details: {
        protocol,
        hosts,
        database: database || undefined,
        options: Object.keys(options).length > 0 ? options : undefined,
      },
    };

  } catch (error) {
    return {
      isValid: false,
      error: `Invalid URL format: ${error instanceof Error ? error.message : 'Unknown error'}`,
    };
  }
}

export async function testMongoDBConnection(
  connectionString: string,
  database?: string,
  additionalOptions?: Record<string, any>
): Promise<ConnectionTestResult> {
  
  // First validate the connection string format
  const parseResult = parseMongoDBConnectionString(connectionString);
  if (!parseResult.isValid) {
    return parseResult;
  }

  try {
    // Test actual connection
    const connection = await mongoose.connect(connectionString, {
      dbName: database || undefined,
      ...additionalOptions,
      // Add timeouts for testing
      serverSelectionTimeoutMS: 10000,
      connectTimeoutMS: 10000,
      socketTimeoutMS: 10000,
    });

    // Test a simple operation
    await connection.connection.db?.admin().ping();
    
    // Close test connection
    await mongoose.disconnect();

    return {
      isValid: true,
      details: parseResult.details,
    };

  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : 'Unknown connection error';
    
    // Provide more specific error messages
    let friendlyError = errorMessage;
    
    if (errorMessage.includes('ENOTFOUND')) {
      friendlyError = 'Host not found. Please check the hostname/IP address.';
    } else if (errorMessage.includes('ECONNREFUSED')) {
      friendlyError = 'Connection refused. Please check if MongoDB is running and the port is correct.';
    } else if (errorMessage.includes('Authentication failed')) {
      friendlyError = 'Authentication failed. Please check username and password.';
    } else if (errorMessage.includes('not authorized')) {
      friendlyError = 'Not authorized. Please check user permissions for the database.';
    } else if (errorMessage.includes('timeout')) {
      friendlyError = 'Connection timeout. Please check network connectivity and firewall settings.';
    }

    return {
      isValid: false,
      error: friendlyError,
      details: parseResult.details,
    };
  }
}

export function validateConnectionStringFormat(connectionString: string): string[] {
  const errors: string[] = [];

  if (!connectionString) {
    errors.push('Connection string is required');
    return errors;
  }

  if (!connectionString.startsWith('mongodb://') && !connectionString.startsWith('mongodb+srv://')) {
    errors.push('Connection string must start with mongodb:// or mongodb+srv://');
  }

  // Use our custom parser instead of URL constructor
  const parseResult = parseMongoDBConnectionString(connectionString);
  if (!parseResult.isValid) {
    errors.push(parseResult.error || 'Invalid connection string format');
    return errors;
  }

  // Check for common issues
  if (connectionString.includes(' ')) {
    errors.push('Connection string should not contain spaces');
  }

  // Check if hosts are present
  if (!parseResult.details?.hosts || parseResult.details.hosts.length === 0) {
    errors.push('At least one host is required in connection string');
  }

  return errors;
}
