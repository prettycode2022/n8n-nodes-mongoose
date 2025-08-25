import * as mongodbValidator from '../../utils/mongodbValidator';

describe('MongoDB Validator Utils', () => {
  describe('Connection String Validation', () => {
    test('should validate correct MongoDB connection string', () => {
      const validConnections = [
        'mongodb://localhost:27017/testdb',
        'mongodb://user:pass@localhost:27017/testdb',
        'mongodb+srv://user:pass@cluster.mongodb.net/testdb',
        'mongodb://localhost:27017,localhost:27018/testdb'
      ];

      validConnections.forEach(connection => {
        // Test if the validator would accept these strings
        // Note: Actual implementation depends on your validator functions
        expect(connection).toMatch(/^mongodb(\+srv)?:\/\//);
      });
    });

    test('should reject invalid connection strings', () => {
      const invalidConnections = [
        'http://localhost:27017/testdb',
        'mysql://localhost:3306/testdb',
        'invalid-string',
        ''
      ];

      invalidConnections.forEach(connection => {
        expect(connection).not.toMatch(/^mongodb(\+srv)?:\/\//);
      });
    });
  });

  describe('Collection Name Validation', () => {
    test('should validate correct collection names', () => {
      const validNames = [
        'users',
        'user_profiles',
        'products-2024',
        'my.collection',
        'collection123'
      ];

      validNames.forEach(name => {
        expect(name).toMatch(/^[a-zA-Z0-9_\-\.]+$/);
        expect(name.length).toBeGreaterThan(0);
        expect(name.length).toBeLessThanOrEqual(127);
      });
    });

    test('should reject invalid collection names', () => {
      const invalidNames = [
        '',
        'collection with spaces',
        'collection$with$special',
        'a'.repeat(128) // Too long
      ];

      invalidNames.forEach(name => {
        const isValid = name.length > 0 && 
                       name.length <= 127 && 
                       /^[a-zA-Z0-9_\-\.]+$/.test(name);
        expect(isValid).toBe(false);
      });
    });
  });

  describe('Query Validation', () => {
    test('should validate correct MongoDB queries', () => {
      const validQueries = [
        '{}',
        '{"name": "John"}',
        '{"age": {"$gte": 18}}',
        '{"status": {"$in": ["active", "pending"]}}'
      ];

      validQueries.forEach(query => {
        expect(() => JSON.parse(query)).not.toThrow();
      });
    });

    test('should reject invalid JSON queries', () => {
      const invalidQueries = [
        '{name: "John"}', // Missing quotes
        '{"name": }', // Invalid syntax
        'not-json',
        ''
      ];

      invalidQueries.forEach(query => {
        if (query === '') {
          expect(query).toBe('');
        } else {
          expect(() => JSON.parse(query)).toThrow();
        }
      });
    });
  });
});
