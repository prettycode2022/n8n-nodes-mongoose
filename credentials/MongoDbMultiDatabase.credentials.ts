import {
  IAuthenticateGeneric,
  ICredentialType,
  INodeProperties,
} from 'n8n-workflow';

export class MongoDbMultiDatabase implements ICredentialType {
  name = 'mongoDbMultiDatabase';
  displayName = 'MongoDB Multi-Database';
  documentationUrl = 'mongodb';
  properties: INodeProperties[] = [
    {
      displayName: 'Connection Type',
      name: 'connectionType',
      type: 'options',
      options: [
        {
          name: 'Single Host',
          value: 'single',
          description: 'Connect to a single MongoDB instance with multiple databases',
        },
        {
          name: 'Replica Set',
          value: 'replicaSet',
          description: 'Connect to a MongoDB replica set with multiple databases',
        },
      ],
      default: 'single',
      description: 'Type of MongoDB connection',
    },
    {
      displayName: 'Host',
      name: 'host',
      type: 'string',
      default: 'localhost',
      description: 'MongoDB host',
      required: true,
      displayOptions: {
        show: {
          connectionType: ['single'],
        },
      },
    },
    {
      displayName: 'Port',
      name: 'port',
      type: 'number',
      default: 27017,
      description: 'MongoDB port',
      required: true,
      displayOptions: {
        show: {
          connectionType: ['single'],
        },
      },
    },
    {
      displayName: 'Hosts',
      name: 'hosts',
      type: 'string',
      default: 'host1:27017,host2:27017,host3:27017',
      description: 'Comma-separated list of MongoDB hosts (host:port)',
      required: true,
      placeholder: 'host1:27017,host2:27018,host3:27019',
      displayOptions: {
        show: {
          connectionType: ['replicaSet'],
        },
      },
    },
    {
      displayName: 'Username',
      name: 'username',
      type: 'string',
      default: '',
      description: 'MongoDB username (leave empty for no authentication)',
    },
    {
      displayName: 'Password',
      name: 'password',
      type: 'string',
      typeOptions: {
        password: true,
      },
      default: '',
      description: 'MongoDB password',
      displayOptions: {
        hide: {
          username: [''],
        },
      },
    },
    {
      displayName: 'Authentication Database',
      name: 'authSource',
      type: 'string',
      default: 'admin',
      description: 'The database to authenticate against (usually "admin")',
      displayOptions: {
        hide: {
          username: [''],
        },
      },
    },
    {
      displayName: 'Replica Set Name',
      name: 'replicaSetName',
      type: 'string',
      default: 'rs0',
      description: 'Name of the replica set',
      required: true,
      displayOptions: {
        show: {
          connectionType: ['replicaSet'],
        },
      },
    },
    {
      displayName: 'Available Databases',
      name: 'databases',
      type: 'string',
      default: 'db1,db2,db3',
      description: 'Comma-separated list of database names that can be accessed with this credential',
      required: true,
      placeholder: 'production,staging,analytics',
    },
    {
      displayName: 'Default Database',
      name: 'defaultDatabase',
      type: 'string',
      default: '',
      description: 'Default database to use if none is specified in the node (optional)',
    },
    {
      displayName: 'Additional Connection Options',
      name: 'options',
      type: 'collection',
      placeholder: 'Add Option',
      default: {},
      description: 'Additional MongoDB connection options',
      options: [
        {
          displayName: 'SSL',
          name: 'ssl',
          type: 'boolean',
          default: false,
          description: 'Whether to use SSL connection',
        },
        {
          displayName: 'SSL Validate',
          name: 'sslValidate',
          type: 'boolean',
          default: true,
          description: 'Whether to validate SSL certificates',
          displayOptions: {
            show: {
              ssl: [true],
            },
          },
        },
        {
          displayName: 'Connection Timeout (Ms)',
          name: 'connectTimeoutMS',
          type: 'number',
          default: 10000,
          description: 'How long to wait for a connection to be established before timing out',
        },
        {
          displayName: 'Socket Timeout (Ms)',
          name: 'socketTimeoutMS',
          type: 'number',
          default: 0,
          description: 'How long to wait for a socket to be established before timing out (0 = no timeout)',
        },
        {
          displayName: 'Max Pool Size',
          name: 'maxPoolSize',
          type: 'number',
          default: 10,
          description: 'Maximum number of connections in the connection pool',
        },
        {
          displayName: 'Min Pool Size',
          name: 'minPoolSize',
          type: 'number',
          default: 0,
          description: 'Minimum number of connections in the connection pool',
        },
        {
          displayName: 'Read Preference',
          name: 'readPreference',
          type: 'options',
          options: [
            {
              name: 'Nearest',
              value: 'nearest',
              description: 'Read from the nearest member',
            },
            {
              name: 'Primary',
              value: 'primary',
              description: 'Read from primary only',
            },
            {
              name: 'Primary Preferred',
              value: 'primaryPreferred',
              description: 'Read from primary if available, otherwise from secondary',
            },
            {
              name: 'Secondary',
              value: 'secondary',
              description: 'Read from secondary only',
            },
            {
              name: 'Secondary Preferred',
              value: 'secondaryPreferred',
              description: 'Read from secondary if available, otherwise from primary',
            },
          ],
          default: 'primary',
          description: 'Read preference for replica set',
        },
        {
          displayName: 'Write Concern',
          name: 'writeConcern',
          type: 'string',
          default: 'majority',
          description: 'Write concern (e.g., "majority", "1", "2")',
        },
      ],
    },
  ];

  authenticate: IAuthenticateGeneric = {
    type: 'generic',
    properties: {},
  };
}
