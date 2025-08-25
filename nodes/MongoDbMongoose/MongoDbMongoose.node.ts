import {
  IExecuteFunctions,
  INodeExecutionData,
  INodeType,
  INodeTypeDescription,
  NodeOperationError,
} from 'n8n-workflow';
import { NodeConnectionType } from 'n8n-workflow/dist/Interfaces';

import mongoose, { Schema, Model, Connection } from 'mongoose';
import {
  validateAndResolveMultiDatabaseCredentials,
  getDatabaseForConnection
} from '../../utils/multiDatabaseHelper';

// Constants for query safety limits
const MAX_QUERY_LIMIT = 10000;
const DEFAULT_QUERY_LIMIT = 1000;



// Helper function to parse JSON parameter (handles both object and string)
function parseJsonParameter(value: any, defaultValue: any = {}): any {
  // If value is null, undefined, or empty string, return default
  if (value === null || value === undefined || value === '') {
    return defaultValue;
  }

  // If value is already an object, return it
  if (typeof value === 'object') {
    return value;
  }

  // If value is a string, try to parse it
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

  // For other types, return as is or default
  return value || defaultValue;
}

// Helper function to convert date strings to Date objects in MongoDB queries
function convertDateStringsToObjects(obj: any, schemaDefinition: any = {}): any {
  if (obj === null || obj === undefined) {
    return obj;
  }

  // If it's an array, process each element
  if (Array.isArray(obj)) {
    return obj.map(item => convertDateStringsToObjects(item, schemaDefinition));
  }

  // If it's not an object, return as is
  if (typeof obj !== 'object') {
    return obj;
  }

  const result: any = {};

  for (const [key, value] of Object.entries(obj)) {
    if (value === null || value === undefined) {
      result[key] = value;
      continue;
    }

    // Check if this field is defined as Date in schema
    const isDateField = schemaDefinition[key]?.type === 'Date' ||
                       (typeof schemaDefinition[key] === 'object' && schemaDefinition[key].type === 'Date');

    // Handle MongoDB operators that might contain date values
    if (key.startsWith('$') && typeof value === 'object' && !Array.isArray(value)) {
      if (['$gte', '$gt', '$lte', '$lt', '$eq', '$ne'].includes(key)) {
        // For comparison operators, convert string dates to Date objects
        if (typeof value === 'string' && isValidDateString(value)) {
          result[key] = new Date(value);
        } else {
          result[key] = value;
        }
      } else {
        // For other operators, recursively process
        result[key] = convertDateStringsToObjects(value, schemaDefinition);
      }
    } else if (isDateField && typeof value === 'object' && !Array.isArray(value)) {
      // If this is a date field with operators, process the operators
      const processedValue: any = {};
      for (const [opKey, opValue] of Object.entries(value)) {
        if (['$gte', '$gt', '$lte', '$lt', '$eq', '$ne'].includes(opKey) &&
            typeof opValue === 'string' && isValidDateString(opValue)) {
          processedValue[opKey] = new Date(opValue);
        } else {
          processedValue[opKey] = convertDateStringsToObjects(opValue, schemaDefinition);
        }
      }
      result[key] = processedValue;
    } else if (isDateField && typeof value === 'string' && isValidDateString(value)) {
      // Direct date field assignment
      result[key] = new Date(value);
    } else if (typeof value === 'object') {
      // Recursively process nested objects
      result[key] = convertDateStringsToObjects(value, schemaDefinition);
    } else {
      result[key] = value;
    }
  }

  return result;
}

// Helper function to check if a string is a valid date string
function isValidDateString(str: string): boolean {
  if (typeof str !== 'string') return false;

  // Check for ISO date format or other common date formats
  const isoDateRegex = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{3})?Z?$/;
  const dateRegex = /^\d{4}-\d{2}-\d{2}$/;

  if (!isoDateRegex.test(str) && !dateRegex.test(str)) {
    return false;
  }

  const date = new Date(str);
  return !isNaN(date.getTime());
}

// Helper function to format response with query info
function formatResponseWithQueryInfo(
  data: any,
  includeQueryInfo: boolean,
  queryInfo: {
    operation: string;
    query?: any;
    options?: any;
    updateData?: any;
    pipeline?: any;
  }
): any {
  if (!includeQueryInfo) {
    return data;
  }

  return {
    query: queryInfo,
    data: data
  };
}





export class MongoDbMongoose implements INodeType {
  description: INodeTypeDescription = {
    displayName: 'MongoDB Mongoose',
    name: 'mongoDbMongoose',
    icon: 'file:mongodb.svg',
    group: ['transform'],
    version: 1,
    subtitle: '={{$parameter["operation"] + ": " + $parameter["collection"]}}',
    description: 'Interact with MongoDB using Mongoose ODM',
    defaults: {
      name: 'MongoDB Mongoose',
    },
    inputs: [NodeConnectionType.Main],
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
        displayName: 'Operation',
        name: 'operation',
        type: 'options',
        noDataExpression: true,
        options: [
          {
            name: 'Aggregate',
            value: 'aggregate',
            description: 'Run aggregation pipeline',
            action: 'Run aggregation',
          },
          {
            name: 'Count',
            value: 'count',
            description: 'Count documents',
            action: 'Count documents',
          },
          {
            name: 'Create',
            value: 'create',
            description: 'Create a new document',
            action: 'Create a document',
          },
          {
            name: 'Delete',
            value: 'delete',
            description: 'Delete documents',
            action: 'Delete documents',
          },
          {
            name: 'Delete One',
            value: 'deleteOne',
            description: 'Delete a single document',
            action: 'Delete one document',
          },
          {
            name: 'Find',
            value: 'find',
            description: 'Find documents',
            action: 'Find documents',
          },
          {
            name: 'Find One',
            value: 'findOne',
            description: 'Find a single document',
            action: 'Find one document',
          },
          {
            name: 'Update',
            value: 'update',
            description: 'Update documents',
            action: 'Update documents',
          },
          {
            name: 'Update One',
            value: 'updateOne',
            description: 'Update a single document',
            action: 'Update one document',
          },
        ],
        default: 'find',
      },
      {
        displayName: 'Collection',
        name: 'collection',
        type: 'string',
        required: true,
        default: '',
        placeholder: 'users',
        description: 'Name of the collection to operate on',
      },
      // Query/Filter
      {
        displayName: 'Query',
        name: 'query',
        type: 'json',
        default: '{}',
        description: 'MongoDB query object (JSON format). Leave empty for no filter.',
        placeholder: '{"name": "John", "age": {"$gte": 18}}',
        displayOptions: {
          show: {
            operation: ['find', 'findOne', 'update', 'updateOne', 'delete', 'deleteOne', 'count'],
          },
        },
      },
      // Schema Definition
      {
        displayName: 'Schema Definition',
        name: 'schemaDefinition',
        type: 'json',
        default: '{}',
        required: true,
        description: 'Define the Mongoose schema for the collection (JSON format)',
        placeholder: '{"name": "String", "email": "String", "age": "Number", "created_at": "Date"}',
      },
      // Select Fields
      {
        displayName: 'Select Fields',
        name: 'selectFields',
        type: 'string',
        default: '',
        description: 'Fields to include/exclude (JSON format). Leave empty to select all fields.',
        placeholder: '{"_id": 1, "name": 1, "email": 1, "created_at": 1}',
        displayOptions: {
          show: {
            operation: ['find', 'findOne'],
          },
        },
      },
      // Include Query Info
      {
        displayName: 'Include Query Info',
        name: 'includeQueryInfo',
        type: 'boolean',
        default: false,
        description: 'Whether to include raw MongoDB query information in the response',
        displayOptions: {
          show: {
            operation: ['find', 'findOne', 'update', 'updateOne', 'delete', 'deleteOne', 'count', 'aggregate'],
          },
        },
      },
      // Document data for create/update
      {
        displayName: 'Document',
        name: 'document',
        type: 'json',
        default: '{}',
        description: 'Document data to create or update',
        displayOptions: {
          show: {
            operation: ['create'],
          },
        },
      },
      {
        displayName: 'Update Data',
        name: 'updateData',
        type: 'json',
        default: '{}',
        description: 'Data to update in the document(s)',
        displayOptions: {
          show: {
            operation: ['update', 'updateOne'],
          },
        },
      },
      // Aggregation pipeline
      {
        displayName: 'Pipeline',
        name: 'pipeline',
        type: 'json',
        default: '[]',
        description: 'Aggregation pipeline stages',
        placeholder: '[{"$match": {"status": 1}}, {"$sort": {"created_at": -1}}, {"$limit": 100}, {"$project": {"name": 1, "email": 1}}]',
        displayOptions: {
          show: {
            operation: ['aggregate'],
          },
        },
      },
      // Options
      {
        displayName: 'Options',
        name: 'options',
        type: 'collection',
        placeholder: 'Add Option',
        default: {},
        options: [
          {
            displayName: 'Limit',
            name: 'limit',
            type: 'number',
            typeOptions: {
              minValue: 1,
              maxValue: 10000,
            },
            default: 1000,
            description: 'Max number of results to return',
          },
          {
            displayName: 'Skip',
            name: 'skip',
            type: 'number',
            default: 0,
            description: 'Number of documents to skip (not applicable for aggregate operations)',
          },
          {
            displayName: 'Sort',
            name: 'sort',
            type: 'json',
            default: '{}',
            description: 'Sort order (e.g., {"name": 1, "age": -1}) - not applicable for aggregate operations',
          },
          {
            displayName: 'Select Fields',
            name: 'select',
            type: 'string',
            default: '',
            description: 'Fields to include/exclude (e.g., "name email -_id") - not applicable for aggregate operations',
          },
          {
            displayName: 'Upsert',
            name: 'upsert',
            type: 'boolean',
            default: false,
            description: 'Whether to create document if it doesn\'t exist (update operations only)',
          },
          {
            displayName: 'Multi',
            name: 'multi',
            type: 'boolean',
            default: false,
            description: 'Whether to update multiple documents (update operations only)',
          },
          {
            displayName: 'Query Timeout (Ms)',
            name: 'maxTimeMS',
            type: 'number',
            typeOptions: {
              minValue: 1000,
              maxValue: 300000,
            },
            default: 30000,
            description: 'Maximum time to allow query to run (1-300 seconds)',
          },
          {
            displayName: 'Debug Mode',
            name: 'debug',
            type: 'boolean',
            default: false,
            description: 'Whether to enable Mongoose debug mode to see all MongoDB queries in console',
          },
        ],
        displayOptions: {
          show: {
            operation: ['find', 'findOne', 'update', 'updateOne', 'delete', 'deleteOne', 'count', 'aggregate'],
          },
        },
      },
    ],
  };

  async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
    const items = this.getInputData();
    const returnData: INodeExecutionData[] = [];
    const operation = this.getNodeParameter('operation', 0) as string;
    const collection = this.getNodeParameter('collection', 0) as string;

    // Get user-specified database parameter
    const userDatabase = this.getNodeParameter('database', 0, '') as string;

    // Check if debug mode is enabled in options
    const optionsParam = this.getNodeParameter('options', 0, {});
    const options = parseJsonParameter(optionsParam, {});
    const debugMode = options.debug || false;

    // Get Multi-Database credentials
    const multiDbCredentials = await this.getCredentials('mongoDbMultiDatabase');
    const multiDbConfig = validateAndResolveMultiDatabaseCredentials(multiDbCredentials, userDatabase);

    const connectionString = multiDbConfig.connectionString;
    const database = getDatabaseForConnection(multiDbConfig, userDatabase);

    // Chỉ log thông tin connection cơ bản khi debug mode được bật
    if (debugMode) {
      console.log('MongoDB connection - Database:', database);
    }

    // Create dedicated connection for this node execution
    let nodeConnection: Connection | null = null;

    try {
      // Prepare connection options for dedicated connection
      const connectionOptions: any = {
        dbName: database || undefined,
        serverSelectionTimeoutMS: 10000,
        connectTimeoutMS: 10000,
        socketTimeoutMS: 10000,
        maxPoolSize: 10,
        minPoolSize: 1,
        maxIdleTimeMS: 30000,
        heartbeatFrequencyMS: 10000,
        retryWrites: true,
        retryReads: true,
      };

      // Add additional options from credentials if any
      const additionalOptions = multiDbCredentials?.options as any || {};
      Object.assign(connectionOptions, additionalOptions);

      // Reduced logging - chỉ log khi cần thiết

      // Create dedicated connection for this node execution
      // This ensures each node has its own connection and won't interfere with others
      nodeConnection = mongoose.createConnection(connectionString, connectionOptions);

      // Wait for connection to be established
      await new Promise((resolve, reject) => {
        const timeout = setTimeout(() => {
          reject(new Error('Connection timeout after 10 seconds'));
        }, 10000);

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

      // Connection established - log reduced để tránh spam

      for (let i = 0; i < items.length; i++) {
        try {
          let model: Model<any>;

          // Get schema definition from user input
          const schemaDefinitionParam = this.getNodeParameter('schemaDefinition', i, {});
          const schemaDefinition = parseJsonParameter(schemaDefinitionParam, {});

          // Create dynamic schema with user-defined fields
          // Set collection name explicitly to prevent Mongoose pluralization
          const schema = new Schema(schemaDefinition, { strict: false, collection: collection });

          // Use dedicated connection to create model
          // This ensures the model uses our dedicated connection, not the global mongoose connection
          try {
            model = nodeConnection!.model(collection, schema);
          } catch {
            // If model already exists on this connection, get it
            model = nodeConnection!.model(collection);
          }

          let result: any;

          switch (operation) {
            case 'create':
              const documentParam = this.getNodeParameter('document', i, {});
              const document = parseJsonParameter(documentParam, {});
              const createResult = await model.create(document);
              
              // Convert ObjectId to string để tránh JSON serialization errors
              result = JSON.parse(JSON.stringify(createResult));
              break;

            case 'find':
              const findQueryParam = this.getNodeParameter('query', i, {});
              let findQuery = parseJsonParameter(findQueryParam, {});
              const findOptionsParam = this.getNodeParameter('options', i, {});
              const findOptions = parseJsonParameter(findOptionsParam, {});
              const selectFieldsParam = this.getNodeParameter('selectFields', i, '');
              const selectFields = parseJsonParameter(selectFieldsParam, null);
              const includeQueryInfo = this.getNodeParameter('includeQueryInfo', i, false) as boolean;

              // Convert date strings to Date objects in query
              findQuery = convertDateStringsToObjects(findQuery, schemaDefinition);



              // Ensure findQuery is an object
              if (typeof findQuery === 'string') {
                throw new Error(`Query parameter is still a string after parsing: ${findQuery}`);
              }

              let findCursor = model.find(findQuery);

              // Enforce safety limits
              let limit = findOptions.limit || DEFAULT_QUERY_LIMIT;

              if (limit > MAX_QUERY_LIMIT) {
                throw new Error(`Limit ${limit} exceeds maximum allowed limit of ${MAX_QUERY_LIMIT} for safety`);
              }

              findCursor = findCursor.limit(limit);

              if (findOptions.skip) findCursor = findCursor.skip(findOptions.skip);
              if (findOptions.sort) findCursor = findCursor.sort(parseJsonParameter(findOptions.sort, {}));

              // Add query timeout
              if (findOptions.maxTimeMS) {
                findCursor = findCursor.maxTimeMS(findOptions.maxTimeMS);
              }

              // Use selectFields first, then fallback to options.select
              if (selectFields) {
                findCursor = findCursor.select(selectFields);
              } else if (findOptions.select) {
                findCursor = findCursor.select(findOptions.select);
              }

              const findResult = await findCursor.lean().exec();
              
              // Convert ObjectId to string để tránh JSON serialization errors
              const jsonSafeFindResult = JSON.parse(JSON.stringify(findResult));

              // Performance warning chỉ khi debug mode
              if (debugMode && Array.isArray(findResult) && findResult.length > 5000) {
                console.warn(`⚠️  Large result set: ${findResult.length} documents returned. Consider adding indexes or more specific filters.`);
              }

              result = formatResponseWithQueryInfo(jsonSafeFindResult, includeQueryInfo, {
                operation: 'find',
                query: findQuery,
                options: {
                  ...findOptions,
                  selectFields: selectFields
                }
              });
              break;

            case 'findOne':
              const findOneQueryParam = this.getNodeParameter('query', i, {});
              const findOneQuery = parseJsonParameter(findOneQueryParam, {});
              const findOneOptionsParam = this.getNodeParameter('options', i, {});
              const findOneOptions = parseJsonParameter(findOneOptionsParam, {});
              const findOneSelectFieldsParam = this.getNodeParameter('selectFields', i, '');
              const findOneSelectFields = parseJsonParameter(findOneSelectFieldsParam, null);
              const findOneIncludeQueryInfo = this.getNodeParameter('includeQueryInfo', i, false) as boolean;

              let findOneCursor = model.findOne(findOneQuery);

              // Use selectFields first, then fallback to options.select
              if (findOneSelectFields) {
                findOneCursor = findOneCursor.select(findOneSelectFields);
              } else if (findOneOptions.select) {
                findOneCursor = findOneCursor.select(findOneOptions.select);
              }

              const findOneResult = await findOneCursor.lean().exec();
              
              // Convert ObjectId to string để tránh JSON serialization errors
              const jsonSafeFindOneResult = JSON.parse(JSON.stringify(findOneResult));

              result = formatResponseWithQueryInfo(jsonSafeFindOneResult, findOneIncludeQueryInfo, {
                operation: 'findOne',
                query: findOneQuery,
                options: {
                  ...findOneOptions,
                  selectFields: findOneSelectFields
                }
              });
              break;

            case 'update':
              const updateQueryParam = this.getNodeParameter('query', i, {});
              let updateQuery = parseJsonParameter(updateQueryParam, {});
              const updateDataParam = this.getNodeParameter('updateData', i, {});
              let updateData = parseJsonParameter(updateDataParam, {});
              const updateOptionsParam = this.getNodeParameter('options', i, {});
              const updateOptions = parseJsonParameter(updateOptionsParam, {});
              const updateIncludeQueryInfo = this.getNodeParameter('includeQueryInfo', i, false) as boolean;

              // Convert date strings to Date objects in query and update data
              updateQuery = convertDateStringsToObjects(updateQuery, schemaDefinition);
              updateData = convertDateStringsToObjects(updateData, schemaDefinition);

              const updateResult = await model.updateMany(updateQuery, updateData, {
                upsert: updateOptions.upsert || false,
              });

              result = formatResponseWithQueryInfo(updateResult, updateIncludeQueryInfo, {
                operation: 'update',
                query: updateQuery,
                updateData: updateData,
                options: updateOptions
              });
              break;

            case 'updateOne':
              const updateOneQueryParam = this.getNodeParameter('query', i, {});
              const updateOneQuery = parseJsonParameter(updateOneQueryParam, {});
              const updateOneDataParam = this.getNodeParameter('updateData', i, {});
              const updateOneData = parseJsonParameter(updateOneDataParam, {});
              const updateOneOptionsParam = this.getNodeParameter('options', i, {});
              const updateOneOptions = parseJsonParameter(updateOneOptionsParam, {});
              const updateOneIncludeQueryInfo = this.getNodeParameter('includeQueryInfo', i, false) as boolean;

              const updateOneResult = await model.updateOne(updateOneQuery, updateOneData, {
                upsert: updateOneOptions.upsert || false,
              });

              result = formatResponseWithQueryInfo(updateOneResult, updateOneIncludeQueryInfo, {
                operation: 'updateOne',
                query: updateOneQuery,
                updateData: updateOneData,
                options: updateOneOptions
              });
              break;

            case 'delete':
              const deleteQueryParam = this.getNodeParameter('query', i, {});
              let deleteQuery = parseJsonParameter(deleteQueryParam, {});
              const deleteIncludeQueryInfo = this.getNodeParameter('includeQueryInfo', i, false) as boolean;

              // Convert date strings to Date objects in query
              deleteQuery = convertDateStringsToObjects(deleteQuery, schemaDefinition);

              const deleteResult = await model.deleteMany(deleteQuery);

              result = formatResponseWithQueryInfo(deleteResult, deleteIncludeQueryInfo, {
                operation: 'delete',
                query: deleteQuery
              });
              break;

            case 'deleteOne':
              const deleteOneQueryParam = this.getNodeParameter('query', i, {});
              let deleteOneQuery = parseJsonParameter(deleteOneQueryParam, {});
              const deleteOneIncludeQueryInfo = this.getNodeParameter('includeQueryInfo', i, false) as boolean;

              // Convert date strings to Date objects in query
              deleteOneQuery = convertDateStringsToObjects(deleteOneQuery, schemaDefinition);

              const deleteOneResult = await model.deleteOne(deleteOneQuery);

              result = formatResponseWithQueryInfo(deleteOneResult, deleteOneIncludeQueryInfo, {
                operation: 'deleteOne',
                query: deleteOneQuery
              });
              break;

            case 'aggregate':
              const pipelineParam = this.getNodeParameter('pipeline', i, []);
              let pipeline = parseJsonParameter(pipelineParam, []);
              const aggregateIncludeQueryInfo = this.getNodeParameter('includeQueryInfo', i, false) as boolean;

              // Convert date strings to Date objects in pipeline stages
              pipeline = pipeline.map((stage: any) => convertDateStringsToObjects(stage, schemaDefinition));

              const aggregateResult = await model.aggregate(pipeline);
              
              // Convert ObjectId to string để tránh JSON serialization errors
              const jsonSafeAggregateResult = JSON.parse(JSON.stringify(aggregateResult));

              result = formatResponseWithQueryInfo(jsonSafeAggregateResult, aggregateIncludeQueryInfo, {
                operation: 'aggregate',
                pipeline: pipeline
              });
              break;

            case 'count':
              const countQueryParam = this.getNodeParameter('query', i, {});
              let countQuery = parseJsonParameter(countQueryParam, {});
              const countIncludeQueryInfo = this.getNodeParameter('includeQueryInfo', i, false) as boolean;

              // Convert date strings to Date objects in query
              countQuery = convertDateStringsToObjects(countQuery, schemaDefinition);

              const countResult = await model.countDocuments(countQuery);

              result = formatResponseWithQueryInfo(countResult, countIncludeQueryInfo, {
                operation: 'count',
                query: countQuery
              });
              break;

            default:
              throw new NodeOperationError(this.getNode(), `Unknown operation: ${operation}`);
          }

          // Handle results - check if it's wrapped with query info
          // If result has query info wrapper, extract the actual data
          if (result && typeof result === 'object' && 'data' in result && 'query' in result) {
            // This is a wrapped result from formatResponseWithQueryInfo
            returnData.push({
              json: result, // Return the whole wrapper object
              pairedItem: { item: i },
            });
          } else if (Array.isArray(result)) {
            // Handle array results (when includeQueryInfo is false)
            // With .lean(), results are already plain objects
            result.forEach((item) => {
              returnData.push({
                json: item,
                pairedItem: { item: i },
              });
            });
          } else if (result) {
            // Handle single result (when includeQueryInfo is false)
            // With .lean(), results are already plain objects
            returnData.push({
              json: result,
              pairedItem: { item: i },
            });
          } else {
            returnData.push({
              json: { result: null },
              pairedItem: { item: i },
            });
          }
        } catch (error) {
          const errorMessage = error instanceof Error ? error.message : 'Unknown error occurred';
          if (this.continueOnFail()) {
            returnData.push({
              json: { error: errorMessage },
              pairedItem: { item: i },
            });
          } else {
            throw new NodeOperationError(this.getNode(), errorMessage, { itemIndex: i });
          }
        }
      }
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error occurred';

      // Provide more specific error messages for common connection issues
      if (errorMessage.includes('MongoNotConnectedError') || errorMessage.includes('Client must be connected')) {
        throw new NodeOperationError(this.getNode(),
          `MongoDB connection error: ${errorMessage}. Please verify your connection string and ensure MongoDB is running and accessible.`);
      } else if (errorMessage.includes('authentication failed') || errorMessage.includes('Authentication failed')) {
        throw new NodeOperationError(this.getNode(),
          `MongoDB authentication failed: ${errorMessage}. Please check your username, password, and authSource settings.`);
      } else if (errorMessage.includes('ENOTFOUND') || errorMessage.includes('getaddrinfo')) {
        throw new NodeOperationError(this.getNode(),
          `MongoDB host not found: ${errorMessage}. Please verify your hostname/IP address and network connectivity.`);
      } else if (errorMessage.includes('ECONNREFUSED')) {
        throw new NodeOperationError(this.getNode(),
          `MongoDB connection refused: ${errorMessage}. Please ensure MongoDB is running on the specified port.`);
      } else {
        throw new NodeOperationError(this.getNode(), `Database connection failed: ${errorMessage}`);
      }
    } finally {
      // Always close the dedicated connection for this node execution
      if (nodeConnection) {
        try {
          await nodeConnection.close();
          // Connection cleanup - không cần log trừ khi có lỗi
        } catch (error) {
          // Log lỗi cleanup vì có thể ảnh hưởng đến performance
          console.warn('Warning: Error closing dedicated MongoDB connection:', error);
        }
      }
    }

    // Debug structure analysis chỉ khi debug mode được bật
    if (debugMode && returnData.length > 0) {
      console.log('🔍 Return data structure:', {
        length: returnData.length,
        format: Array.isArray(returnData) ? 'Array' : typeof returnData
      });
    }

    return [returnData];
  }
}
