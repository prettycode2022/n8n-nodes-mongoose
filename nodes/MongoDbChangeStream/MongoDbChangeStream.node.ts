import {
  ITriggerFunctions,
  INodeType,
  INodeTypeDescription,
  ITriggerResponse,
  NodeOperationError,
  LoggerProxy,
} from 'n8n-workflow';
import { NodeConnectionType } from 'n8n-workflow/dist/Interfaces';

import mongoose, { Model, Schema, Connection } from 'mongoose';
import { ChangeStream } from 'mongodb';
import {
  validateAndResolveMultiDatabaseCredentials,
  getDatabaseForConnection
} from '../../utils/multiDatabaseHelper';



// Helper function to parse JSON parameter
function parseJsonParameter(value: any, defaultValue: any = {}): any {
  if (value === null || value === undefined || value === '') {
    return defaultValue;
  }

  if (typeof value === 'object') {
    return value;
  }

  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (trimmed === '') {
      return defaultValue;
    }

    try {
      return JSON.parse(trimmed);
    } catch (error) {
      throw new Error(`Invalid JSON format: ${trimmed}. Error: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  return value;
}

export class MongoDbChangeStream implements INodeType {
  description: INodeTypeDescription = {
    displayName: 'MongoDB Mongoose Change Stream',
    name: 'mongoDbChangeStream',
    icon: 'file:mongodb.svg',
    group: ['trigger'],
    version: 1,
    description: 'Real-time MongoDB Change Stream trigger for monitoring collection, database, or deployment changes with Mongoose ODM',
    defaults: {
      name: 'MongoDB Mongoose Change Stream',
    },
    inputs: [],
    outputs: [NodeConnectionType.Main],
    credentials: [
      {
        name: 'mongoDbMultiDatabase',
        required: true,
      },
    ],


    properties: [
      {
        displayName: 'Database',
        name: 'database',
        type: 'string',
        default: '',
        description: 'Database name (leave empty to use default from credential)',
        displayOptions: {
          show: {
            '@version': [1],
          },
        },
      },
      {
        displayName: 'Collection',
        name: 'collection',
        type: 'string',
        default: '',
        required: true,
        placeholder: 'users',
        description: 'Name of the MongoDB collection to monitor for real-time changes (e.g., "users", "orders")',
      },
      {
        displayName: 'Watch Level',
        name: 'watchLevel',
        type: 'options',
        options: [
          {
            name: 'Collection',
            value: 'collection',
            description: 'Watch changes in a specific collection',
          },
          {
            name: 'Database',
            value: 'database',
            description: 'Watch changes in the entire database',
          },
          {
            name: 'Deployment',
            value: 'deployment',
            description: 'Watch changes across all databases',
          },
        ],
        default: 'collection',
        description: 'Level at which to watch for changes',
      },
      {
        displayName: 'Operation Types',
        name: 'operationTypes',
        type: 'multiOptions',
        options: [
          {
            name: 'Insert',
            value: 'insert',
            description: 'Document insertions',
          },
          {
            name: 'Update',
            value: 'update',
            description: 'Document updates',
          },
          {
            name: 'Delete',
            value: 'delete',
            description: 'Document deletions',
          },
          {
            name: 'Replace',
            value: 'replace',
            description: 'Document replacements',
          },
        ],
        default: ['insert', 'update', 'delete', 'replace'],
        description: 'Types of operations to watch for',
      },
      {
        displayName: 'Schema Definition',
        name: 'schemaDefinition',
        type: 'json',
        default: '{}',
        description: 'Mongoose schema definition for proper field type conversion and validation (JSON format). Leave empty to use schemaless mode.',
        placeholder: '{"name": {"type": "String"}, "email": {"type": "String"}, "created_at": {"type": "Date"}, "is_active": {"type": "Boolean"}}',
      },
      {
        displayName: 'Match Filter',
        name: 'matchFilter',
        type: 'json',
        default: '{}',
        description: 'MongoDB aggregation match filter to only trigger on specific changes (JSON format). Use "fullDocument.field" to filter by document fields.',
        placeholder: '{"fullDocument.status": "active", "operationType": {"$in": ["insert", "update"]}}',
      },
      {
        displayName: 'Projection',
        name: 'projection',
        type: 'json',
        default: '{}',
        description: 'MongoDB projection to select specific fields in the change event output (JSON format). Use 1 to include, 0 to exclude fields. Reduces data transfer.',
        placeholder: '{"fullDocument.name": 1, "fullDocument.email": 1, "operationType": 1, "clusterTime": 1}',
      },
      {
        displayName: 'Full Document',
        name: 'fullDocument',
        type: 'options',
        options: [
          {
            name: 'Default',
            value: 'default',
            description: 'Default behavior (no full document for deletes)',
          },
          {
            name: 'Update Lookup',
            value: 'updateLookup',
            description: 'Return full document for update operations',
          },
          {
            name: 'When Available',
            value: 'whenAvailable',
            description: 'Return full document when available',
          },
          {
            name: 'Required',
            value: 'required',
            description: 'Always return full document (may fail for some operations)',
          },
        ],
        default: 'default',
        description: 'How to handle full document in change events',
      },
      {
        displayName: 'Output Format',
        name: 'outputFormat',
        type: 'options',
        options: [
          {
            name: 'Full Change Event',
            value: 'full',
            description: 'Complete change stream event with all metadata',
          },
          {
            name: 'Document Only',
            value: 'document',
            description: 'Only the changed document (fullDocument)',
          },
          {
            name: 'Simplified',
            value: 'simplified',
            description: 'Simplified format with operation type and document',
          },
        ],
        default: 'full',
        description: 'Format of the output data',
      },
      {
        displayName: 'Options',
        name: 'options',
        type: 'collection',
        placeholder: 'Add Option',
        default: {},
        options: [
          {
            displayName: 'Batch Size',
            name: 'batchSize',
            type: 'number',
            default: 100,
            description: 'Number of change events to process in each batch (higher = better performance, lower = more real-time)',
          },
          {
            displayName: 'Max Await Time (Ms)',
            name: 'maxAwaitTimeMS',
            type: 'number',
            default: 1000,
            description: 'Maximum time (in milliseconds) to wait for new changes before checking again',
          },
          {
            displayName: 'Resume Token',
            name: 'resumeAfter',
            type: 'string',
            default: '',
            description: 'Resume token (JSON string) to continue watching from a specific point after interruption. Leave empty for new stream.',
          },
          {
            displayName: 'Resume Token From Database',
            name: 'resumeFromDB',
            type: 'boolean',
            default: false,
            description: 'Whether to automatically load resume token from a database collection (resume_tokens)',
          },
          {
            displayName: 'Resume Token Collection',
            name: 'resumeTokenCollection',
            type: 'string',
            default: '',
            placeholder: '={{$parameter["collection"] + "_resume_tokens"}}',
            description: 'Collection name to store/load resume tokens (auto-generated from watched collection)',
            displayOptions: {
              show: {
                resumeFromDB: [true],
              },
            },
          },
          {
            displayName: 'Resume Token Database',
            name: 'resumeTokenDatabase',
            type: 'string',
            default: '',
            description: 'Database for resume tokens (leave empty to use same database as Change Stream)',
            displayOptions: {
              show: {
                resumeFromDB: [true],
              },
            },
          },
          {
            displayName: 'Resume Token Key',
            name: 'resumeTokenKey',
            type: 'string',
            default: '',
            placeholder: 'Auto-generated: {database}.{collection}',
            description: 'Unique key to identify this stream (auto-generated from resolved database + collection)',
            displayOptions: {
              show: {
                resumeFromDB: [true],
              },
            },
          },
          {
            displayName: 'Start At Operation Time',
            name: 'startAtOperationTime',
            type: 'string',
            default: '',
            description: 'Start watching from a specific timestamp (ISO 8601 format). Leave empty to start from current time.',
          },
          {
            displayName: 'Resume Token Save Frequency',
            name: 'resumeTokenSaveFrequency',
            type: 'options',
            options: [
              {
                name: 'Every Change',
                value: 'every_change',
                description: 'Save resume token on every change (more real-time, higher database load)'
              },
              {
                name: 'Smart (Skip Duplicates)',
                value: 'smart',
                description: 'Only save when token actually changes (recommended)'
              },
              {
                name: 'Throttled (5 Seconds)',
                value: 'throttled_5s',
                description: 'Save at most once every 5 seconds (lower database load)'
              }
            ],
            default: 'smart',
            description: 'How frequently to save resume tokens to database',
            displayOptions: {
              show: {
                resumeFromDB: [true],
              },
            },
          },
          {
            displayName: 'Debug Mode',
            name: 'debug',
            type: 'boolean',
            default: false,
            description: 'Whether to enable debug logging for change stream events',
          },
        ],
      },
    ],
  };

  async trigger(this: ITriggerFunctions): Promise<ITriggerResponse> {
    // Get user-specified database parameter
    const userDatabase = this.getNodeParameter('database', '') as string;
    const collection = this.getNodeParameter('collection') as string;
    const watchLevel = this.getNodeParameter('watchLevel') as string;
    const operationTypes = this.getNodeParameter('operationTypes') as string[];
    const schemaDefinitionParam = this.getNodeParameter('schemaDefinition', {});
    const schemaDefinition = parseJsonParameter(schemaDefinitionParam, {});

    // Get Multi-Database credentials
    const multiDbCredentials = await this.getCredentials('mongoDbMultiDatabase');
    const multiDbConfig = validateAndResolveMultiDatabaseCredentials(multiDbCredentials, userDatabase);

    const connectionString = multiDbConfig.connectionString;
    const database = getDatabaseForConnection(multiDbConfig, userDatabase);

    // Get options first để sử dụng cho debug logging
    const optionsParam = this.getNodeParameter('options', {});
    const options = parseJsonParameter(optionsParam, {});

    // Chỉ log khi debug mode được bật
    if (options.debug) {
      LoggerProxy.debug('Using Multi-Database credential for Change Stream');
      LoggerProxy.debug('Available databases: ' + multiDbConfig.availableDatabases.join(', '));
      LoggerProxy.debug('Selected database: ' + database);
    }
    
    const matchFilterParam = this.getNodeParameter('matchFilter', {});
    const matchFilter = parseJsonParameter(matchFilterParam, {});
    const projectionParam = this.getNodeParameter('projection', {});
    const projection = parseJsonParameter(projectionParam, {});
    const fullDocument = this.getNodeParameter('fullDocument') as string;
    const outputFormat = this.getNodeParameter('outputFormat') as string;

    // Check if we should load resume token from database (from options collection)
    const resumeFromDB = options.resumeFromDB || false;
    
    // Smart default for resume token collection - use watching collection name + suffix
    const defaultResumeTokenCollection = `${collection}_resume_tokens`;
    const resumeTokenCollection = options.resumeTokenCollection || defaultResumeTokenCollection;
    
    const resumeTokenDatabase = options.resumeTokenDatabase || '';
    let resumeTokenKey = options.resumeTokenKey || '';
    
    if (options.debug) {
      LoggerProxy.debug('Resume token parameters loaded from options:', {
        resumeFromDB,
        resumeTokenCollection: {
          userInput: options.resumeTokenCollection || '(empty)',
          resolved: resumeTokenCollection,
          default: defaultResumeTokenCollection
        },
        resumeTokenDatabase: {
          userInput: options.resumeTokenDatabase || '(empty)',
          willResolveTo: resumeTokenDatabase || database
        },
        resumeTokenKey: resumeTokenKey || '(empty - will auto-generate)'
      });
    }
    
    // Auto-generate resume token key if not provided or is default expression
    if (resumeFromDB && (!resumeTokenKey || resumeTokenKey.includes('={{') || resumeTokenKey === 'default')) {
      // Use resolved database (from credentials) for accurate key generation
      const resolvedDatabase = database || 'default';
      resumeTokenKey = `${resolvedDatabase}.${collection}`;
      
      if (options.debug) {
        LoggerProxy.debug('Auto-generated resume token key:', { 
          originalKey: options.resumeTokenKey || '',
          generatedKey: resumeTokenKey,
          userDatabase: userDatabase || '(empty)',
          resolvedDatabase: resolvedDatabase,
          collection: collection
        });
      }
    }
    
    // Store database name for use in event handlers  
    const finalResumeTokenDatabase = resumeTokenDatabase || database;
    
    if (options.debug && resumeFromDB) {
      LoggerProxy.debug('Final resume token configuration:', {
        database: finalResumeTokenDatabase,
        collection: resumeTokenCollection,
        key: resumeTokenKey,
        saveFrequency: options.resumeTokenSaveFrequency || 'smart',
        source: {
          databaseFrom: resumeTokenDatabase ? 'user_input' : 'credentials_default',
          collectionFrom: options.resumeTokenCollection ? 'user_input' : 'auto_generated',
          keyFrom: options.resumeTokenKey ? 'user_input' : 'auto_generated'
        },
        performance: {
          expectedBehavior: (() => {
            const freq = options.resumeTokenSaveFrequency || 'smart';
            switch (freq) {
              case 'every_change': return 'Save on every resume token change (highest DB load)';
              case 'smart': return 'Skip saves when token is identical (recommended)';
              case 'throttled_5s': return 'Save at most once per 5 seconds (lowest DB load)';
              default: return 'Unknown frequency setting';
            }
          })()
        }
      });
    }

    let nodeConnection: Connection | null = null;
    let changeStream: ChangeStream | null = null;
    let isClosing = false;

    const closeFunction = async () => {
      if (isClosing) return;
      isClosing = true;

      if (options.debug) {
        LoggerProxy.debug('Closing MongoDB Change Stream connection');
      }

      try {
        if (changeStream) {
          await changeStream.close();
          changeStream = null;
        }
        if (nodeConnection) {
          await nodeConnection.close();
          nodeConnection = null;
        }
      } catch (error) {
        LoggerProxy.error('Error closing MongoDB connection', { error });
      }
    };

    try {

      // Connect to MongoDB with robust options for triggers
      const connectionOptions: any = {
        dbName: database || undefined,
        serverSelectionTimeoutMS: 30000,
        connectTimeoutMS: 30000,
        socketTimeoutMS: 0, // Disable for change streams (like WebSocket)
        maxPoolSize: 10,
        minPoolSize: 1,
        maxIdleTimeMS: 30000,
        heartbeatFrequencyMS: 10000,
        retryWrites: true,
        retryReads: true,
      };

      // Set Mongoose-specific options
      if (options.debug) {
        // NOTE: mongoose.set('debug', true) tạo ra logs cực kỳ dài (8MB/dòng)
        // Đã comment out để tránh spam logs
        // mongoose.set('debug', true);
        LoggerProxy.debug('Debug mode enabled for Change Stream (mongoose global debug disabled to prevent log spam)');
      }

      if (options.debug) {
        LoggerProxy.debug('Creating dedicated MongoDB connection for Change Stream');
      }

      // Create dedicated connection for this Change Stream
      nodeConnection = mongoose.createConnection(connectionString, connectionOptions);

      // Wait for connection to be established
      await new Promise((resolve, reject) => {
        const timeout = setTimeout(() => {
          reject(new Error('Change Stream connection timeout after 30 seconds'));
        }, 30000);

        if (nodeConnection!.readyState === 1) {
          clearTimeout(timeout);
          resolve(undefined);
          return;
        }

        nodeConnection!.once('connected', () => {
          clearTimeout(timeout);
          resolve(undefined);
        });

        nodeConnection!.once('error', (error) => {
          clearTimeout(timeout);
          reject(error);
        });
      });

      // Verify connection is ready
      if (!nodeConnection.db) {
        throw new Error('Database connection not available');
      }
      await nodeConnection.db.admin().ping();

      if (options.debug) {
        LoggerProxy.debug('Dedicated MongoDB connection established and verified for change stream');
      }

      // Load resume token from database if enabled
      if (resumeFromDB && nodeConnection.db) {
        if (options.debug) {
          LoggerProxy.debug('Attempting to load resume token from database...', {
            database: finalResumeTokenDatabase,
            collection: resumeTokenCollection,
            key: resumeTokenKey,
            usingDefaultDatabase: !resumeTokenDatabase
          });
        }
        
        try {
          // Use specified database for resume tokens or fallback to Change Stream database
          const tokenDb = resumeTokenDatabase ? 
            nodeConnection.useDb(resumeTokenDatabase) : 
            nodeConnection.db;
          
          const collection = tokenDb.collection(resumeTokenCollection);
          
          const savedToken = await collection.findOne({ key: resumeTokenKey });
          if (savedToken && savedToken.token) {
            options.resumeAfter = typeof savedToken.token === 'string' 
              ? savedToken.token 
              : JSON.stringify(savedToken.token);
            
            if (options.debug) {
              LoggerProxy.debug('✅ FOUND and loaded resume token from database:', { 
                database: finalResumeTokenDatabase,
                collection: resumeTokenCollection,
                key: resumeTokenKey, 
                token: savedToken.token,
                resumeAfterValue: options.resumeAfter,
                willBeCachedForComparison: true
              });
            }
          } else if (options.debug) {
            LoggerProxy.debug('❌ No saved resume token found in database (first run):', {
              database: finalResumeTokenDatabase,
              collection: resumeTokenCollection,
              key: resumeTokenKey,
              savedTokenExists: !!savedToken
            });
          }
        } catch (error) {
          LoggerProxy.error('Error loading resume token from database:', { error });
        }
      } else if (options.debug) {
        LoggerProxy.debug('Resume token DB loading skipped:', {
          resumeFromDB,
          hasConnection: !!nodeConnection,
          hasDb: !!(nodeConnection && nodeConnection.db)
        });
      }

      // Create model with schema if provided using dedicated connection
      let model: Model<any>;
      if (Object.keys(schemaDefinition).length > 0) {
        const schema = new Schema(schemaDefinition, { strict: false, collection: collection });
        try {
          model = nodeConnection.model(collection, schema);
        } catch {
          model = nodeConnection.model(collection);
        }
      } else {
        // Create model without schema
        const schema = new Schema({}, { strict: false, collection: collection });
        try {
          model = nodeConnection.model(collection, schema);
        } catch {
          model = nodeConnection.model(collection);
        }
      }

      // Build change stream pipeline
      const pipeline: any[] = [];

      // Add operation type filter
      if (operationTypes.length > 0) {
        pipeline.push({
          $match: {
            operationType: { $in: operationTypes }
          }
        });
      }

      // Add custom match filter
      if (Object.keys(matchFilter).length > 0) {
        pipeline.push({
          $match: matchFilter
        });
      }

      // Add projection
      if (Object.keys(projection).length > 0) {
        pipeline.push({
          $project: projection
        });
      }

      // Build change stream options
      const changeStreamOptions: any = {
        fullDocument: fullDocument !== 'default' ? fullDocument : undefined,
        batchSize: options.batchSize || 100,
        maxAwaitTimeMS: options.maxAwaitTimeMS || 1000,
      };

      // Add resume token if provided
      if (options.resumeAfter) {
        try {
          changeStreamOptions.resumeAfter = JSON.parse(options.resumeAfter);
          if (options.debug) {
            LoggerProxy.debug('Using resume token (parsed from JSON):', { resumeAfter: changeStreamOptions.resumeAfter });
          }
        } catch {
          changeStreamOptions.resumeAfter = options.resumeAfter;
          if (options.debug) {
            LoggerProxy.debug('Using resume token (as string):', { resumeAfter: changeStreamOptions.resumeAfter });
          }
        }
      } else if (options.debug) {
        LoggerProxy.debug('No resume token provided - starting fresh stream');
      }

      // Add start at operation time if provided
      if (options.startAtOperationTime) {
        changeStreamOptions.startAtOperationTime = new Date(options.startAtOperationTime);
      }

      // Create change stream based on watch level using dedicated connection
      if (watchLevel === 'collection') {
        changeStream = model.watch(pipeline, changeStreamOptions) as any;
        if (options.debug) {
          LoggerProxy.debug(`Change stream created for collection: ${collection}`);
        }
      } else if (watchLevel === 'database') {
        if (!nodeConnection.db) {
          throw new Error('Database connection not available for database watch');
        }
        changeStream = nodeConnection.db.watch(pipeline, changeStreamOptions) as any;
        if (options.debug) {
          LoggerProxy.debug(`Change stream created for database: ${database}`);
        }
      } else {
        changeStream = nodeConnection.watch(pipeline, changeStreamOptions) as any;
        if (options.debug) {
          LoggerProxy.debug('Change stream created for deployment');
        }
      }

      // Extract loaded token string for caching
      let loadedTokenString: string | null = null;
      if (resumeFromDB && options.resumeAfter) {
        loadedTokenString = options.resumeAfter;
        
        if (options.debug) {
          LoggerProxy.debug('Resume token cached for comparison:', {
            cachedToken: loadedTokenString?.substring(0, 50) + '...',
            source: 'loaded_from_database'
          });
        }
      }

      // Capture variables for event handlers to avoid scope issues
      const capturedOptions = { ...options };
      const capturedResumeFromDB = resumeFromDB;
      const capturedResumeTokenCollection = resumeTokenCollection;
      const capturedResumeTokenDatabase = resumeTokenDatabase;
      const capturedResumeTokenKey = resumeTokenKey;
      const capturedFinalResumeTokenDatabase = finalResumeTokenDatabase;
      
      // Cache for resume token to avoid unnecessary saves
      let lastSavedToken: string | null = loadedTokenString;
      let lastSaveTime: number = 0;
      const saveFrequency = options.resumeTokenSaveFrequency || 'smart';

      // Handle change events
      if (changeStream) {
        changeStream.on('change', (change: any) => {
        try {
          let outputData: any;

          switch (outputFormat) {
            case 'document':
              outputData = change.fullDocument || change.documentKey || {};
              break;
            case 'simplified':
              outputData = {
                operationType: change.operationType || 'unknown',
                documentKey: change.documentKey || {},
                document: change.fullDocument || null,
                updateDescription: change.updateDescription || null,
                timestamp: change.clusterTime || new Date(),
              };
              break;
            default: // 'full'
              outputData = change || {};
              break;
          }

          // Ensure output data is always a valid object
          if (outputData === null || outputData === undefined) {
            outputData = {
              operationType: change?.operationType || 'unknown',
              timestamp: new Date(),
              error: 'No data available'
            };
          }

          this.emit([this.helpers.returnJsonArray([outputData])]);

          // Log change events với tần suất giảm để tránh spam
          if (capturedOptions.debug && Math.random() < 0.1) { // Chỉ log 10% events
            LoggerProxy.debug(`Change stream event: ${change.operationType}`);
          }
        } catch (error) {
          LoggerProxy.error('Error processing change stream event', { error: error instanceof Error ? error.message : String(error) });

          // Emit safe error output instead of failing
          this.emit([this.helpers.returnJsonArray([{
            operationType: 'error',
            error: error instanceof Error ? error.message : String(error),
            timestamp: new Date(),
            type: 'processing_error'
          }])]);
        }
        });

        changeStream.on('error', (error: any) => {
        if (!isClosing) {
          LoggerProxy.error('Change stream error', { error });

          // For certain errors, we might want to emit them as workflow data
          if (capturedOptions.debug) {
            this.emit([this.helpers.returnJsonArray([{
              error: error.message,
              type: 'change_stream_error',
              timestamp: new Date().toISOString()
            }])]);
          }
        }
        });

        changeStream.on('close', () => {
        if (!isClosing && capturedOptions.debug) {
          LoggerProxy.debug('Change stream closed unexpectedly');
        }
        });

        changeStream.on('resumeTokenChanged', async (token: any) => {
        // Resume token changes rất thường xuyên, chỉ log khi cần thiết
        if (capturedOptions.debug && Math.random() < 0.05) { // Chỉ log 5% token changes
          LoggerProxy.debug('Resume token changed');
        }
        
        // Save resume token to database if enabled
        if (capturedResumeFromDB && nodeConnection && nodeConnection.db) {
          try {
            // Convert token to string for comparison
            const tokenString = typeof token === 'string' ? token : JSON.stringify(token);
            const now = Date.now();
            
            // Apply frequency-based save logic
            if (saveFrequency === 'smart' && lastSavedToken === tokenString) {
                          // Token save skip - giảm log spam
            if (capturedOptions.debug && Math.random() < 0.01) { // Chỉ log 1% skips
              LoggerProxy.debug('Resume token save skipped - no change detected');
            }
              return;
            }
            
            if (saveFrequency === 'throttled_5s' && (now - lastSaveTime) < 5000) {
              // Throttled save skip - không cần log thường xuyên
              return;
            }
            
            // Save token check - chỉ log khi thực sự cần thiết
            if (capturedOptions.debug && Math.random() < 0.1) { // 10% token saves
              LoggerProxy.debug('Saving resume token to database');
            }
            
            // Use specified database for resume tokens or fallback to Change Stream database
            const tokenDb = capturedResumeTokenDatabase ? 
              nodeConnection.useDb(capturedResumeTokenDatabase) : 
              nodeConnection.db;
            
            const collection = tokenDb.collection(capturedResumeTokenCollection);
            
            const result = await collection.replaceOne(
              { key: capturedResumeTokenKey },
              { 
                key: capturedResumeTokenKey,
                token: token, 
                updatedAt: new Date() 
              },
              { upsert: true }
            );
            
            // Resume token saved - log minimal để tránh spam
            if (capturedOptions.debug && result.upsertedCount > 0) { // Chỉ log lần đầu save
              LoggerProxy.debug('Resume token initialized in database');
            }
            
            // Update cache and timing only after successful save
            lastSavedToken = tokenString;
            lastSaveTime = now;
          } catch (error) {
            LoggerProxy.error('Error saving resume token to database', { error });
          }
        } else if (capturedOptions.debug) {
          LoggerProxy.debug('Resume token save skipped:', {
            reason: !capturedResumeFromDB ? 'resumeFromDB disabled' : 
                   !nodeConnection ? 'no nodeConnection' :
                   !nodeConnection.db ? 'no database connection' : 'unknown'
          });
        }
        });
      }

      // Handle MongoDB connection events for dedicated connection
      nodeConnection.on('error', (error: any) => {
        if (!isClosing) {
          LoggerProxy.error('MongoDB connection error', { error });
        }
      });

      nodeConnection.on('disconnected', () => {
        if (!isClosing && options.debug) {
          LoggerProxy.debug('MongoDB disconnected');
        }
      });

      nodeConnection.on('reconnected', () => {
        if (options.debug) {
          LoggerProxy.debug('MongoDB reconnected');
        }
      });

      // Log successful initialization
      if (options.debug) {
        LoggerProxy.debug(`MongoDB Change Stream trigger initialized successfully for ${watchLevel}: ${collection}`);
      }

      return {
        closeFunction,
      };

    } catch (error) {
      await closeFunction();
      throw new NodeOperationError(
        this.getNode(),
        `Failed to initialize change stream: ${error instanceof Error ? error.message : String(error)}`
      );
    }
  }
}
