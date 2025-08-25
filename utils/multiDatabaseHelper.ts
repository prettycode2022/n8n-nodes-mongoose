import { ICredentialDataDecryptedObject } from 'n8n-workflow';

// Types for Multi-Database MongoDB credentials
export interface IMultiDatabaseCredentials {
  connectionString: string;
  availableDatabases: string[];
  defaultDatabase?: string;
}

// Helper function to build connection string for multi-database credential
export function buildMultiDatabaseConnectionString(credentials: ICredentialDataDecryptedObject): string {
  const connectionType = credentials.connectionType as string || 'single';
  
  if (connectionType === 'replicaSet') {
    return buildReplicaSetConnectionString(credentials);
  } else {
    return buildSingleHostConnectionString(credentials);
  }
}

// Helper function to build single host connection string
function buildSingleHostConnectionString(credentials: ICredentialDataDecryptedObject): string {
  const host = credentials.host as string;
  const port = credentials.port as number;
  const username = credentials.username as string;
  const password = credentials.password as string;
  const authSource = credentials.authSource as string;
  
  // Build auth part
  const authPart = username && password ? `${username}:${password}@` : '';
  
  // Build query parameters
  const optionParts: string[] = [];
  
  // Add auth source if provided
  if (authSource && username) {
    optionParts.push(`authSource=${authSource}`);
  }
  
  // Add additional options from credentials
  const additionalOptions = credentials.options as any || {};
  Object.keys(additionalOptions).forEach(key => {
    if (additionalOptions[key] !== undefined && additionalOptions[key] !== null) {
      optionParts.push(`${key}=${additionalOptions[key]}`);
    }
  });
  
  const queryString = optionParts.length > 0 ? `?${optionParts.join('&')}` : '';
  
  return `mongodb://${authPart}${host}:${port}${queryString}`;
}

// Helper function to build replica set connection string
function buildReplicaSetConnectionString(credentials: ICredentialDataDecryptedObject): string {
  const hosts = credentials.hosts as string;
  const username = credentials.username as string;
  const password = credentials.password as string;
  const replicaSetName = credentials.replicaSetName as string;
  const authSource = credentials.authSource as string;
  
  // Build auth part
  const authPart = username && password ? `${username}:${password}@` : '';
  
  // Build query parameters
  const optionParts: string[] = [];
  
  // Add replica set name
  if (replicaSetName) {
    optionParts.push(`replicaSet=${replicaSetName}`);
  }
  
  // Add auth source if provided
  if (authSource && username) {
    optionParts.push(`authSource=${authSource}`);
  }
  
  // Add additional options from credentials
  const additionalOptions = credentials.options as any || {};
  Object.keys(additionalOptions).forEach(key => {
    if (additionalOptions[key] !== undefined && additionalOptions[key] !== null) {
      optionParts.push(`${key}=${additionalOptions[key]}`);
    }
  });
  
  const queryString = optionParts.length > 0 ? `?${optionParts.join('&')}` : '';
  
  return `mongodb://${authPart}${hosts}${queryString}`;
}

// Helper function to validate and resolve multi-database credentials
export function validateAndResolveMultiDatabaseCredentials(
  credentials: ICredentialDataDecryptedObject,
  requestedDatabase?: string,
): IMultiDatabaseCredentials {
  if (!credentials) {
    throw new Error('No credentials provided');
  }
  
  // Parse available databases
  const databasesString = credentials.databases as string;
  if (!databasesString) {
    throw new Error('Available databases list is required');
  }
  
  const availableDatabases = databasesString
    .split(',')
    .map(db => db.trim())
    .filter(db => db.length > 0);
  
  if (availableDatabases.length === 0) {
    throw new Error('At least one database must be specified in available databases');
  }
  
  // Get default database
  const defaultDatabase = (credentials.defaultDatabase as string)?.trim() || availableDatabases[0];
  
  // Validate requested database if provided
  if (requestedDatabase) {
    const trimmedRequested = requestedDatabase.trim();
    if (!availableDatabases.includes(trimmedRequested)) {
      throw new Error(
        `Database "${trimmedRequested}" is not in the list of available databases: ${availableDatabases.join(', ')}`
      );
    }
  }
  
  // Build connection string (without database - will be specified per connection)
  const connectionString = buildMultiDatabaseConnectionString(credentials);
  
  return {
    connectionString,
    availableDatabases,
    defaultDatabase,
  };
}

// Helper function to get database name for connection
export function getDatabaseForConnection(
  multiDbCredentials: IMultiDatabaseCredentials,
  requestedDatabase?: string,
): string {
  if (requestedDatabase) {
    const trimmedRequested = requestedDatabase.trim();
    if (!multiDbCredentials.availableDatabases.includes(trimmedRequested)) {
      throw new Error(
        `Database "${trimmedRequested}" is not in the list of available databases: ${multiDbCredentials.availableDatabases.join(', ')}`
      );
    }
    return trimmedRequested;
  }
  
  return multiDbCredentials.defaultDatabase || multiDbCredentials.availableDatabases[0] || '';
}

// Helper function to list available databases from credentials
export function getAvailableDatabases(credentials: ICredentialDataDecryptedObject): string[] {
  const databasesString = credentials.databases as string;
  if (!databasesString) {
    return [];
  }
  
  return databasesString
    .split(',')
    .map(db => db.trim())
    .filter(db => db.length > 0);
}
