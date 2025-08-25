import { MongoDbChangeStream } from '../../nodes/MongoDbChangeStream/MongoDbChangeStream.node';

describe('MongoDbChangeStream Node', () => {
  let mongoDbChangeStream: MongoDbChangeStream;

  beforeEach(() => {
    mongoDbChangeStream = new MongoDbChangeStream();
  });

  describe('Node Description', () => {
    test('should have correct display name', () => {
      expect(mongoDbChangeStream.description.displayName).toBe('MongoDB Mongoose Change Stream');
    });

    test('should have correct node name', () => {
      expect(mongoDbChangeStream.description.name).toBe('mongoDbChangeStream');
    });

    test('should be a trigger node', () => {
      expect(mongoDbChangeStream.description.group).toContain('trigger');
    });

    test('should have correct icon', () => {
      expect(mongoDbChangeStream.description.icon).toBe('file:mongodb.svg');
    });

    test('should have no inputs (trigger node)', () => {
      expect(mongoDbChangeStream.description.inputs).toEqual([]);
    });

    test('should have main output connection', () => {
      expect(mongoDbChangeStream.description.outputs).toEqual(['main']);
    });
  });

  describe('Node Properties', () => {
    test('should have collection property', () => {
      const collectionProperty = mongoDbChangeStream.description.properties.find(
        (prop: any) => prop.name === 'collection'
      );
      expect(collectionProperty).toBeDefined();
      expect(collectionProperty?.type).toBe('string');
    });

    test('should have watchLevel property', () => {
      const watchLevelProperty = mongoDbChangeStream.description.properties.find(
        (prop: any) => prop.name === 'watchLevel'
      );
      expect(watchLevelProperty).toBeDefined();
      expect(watchLevelProperty?.type).toBe('options');
    });

    test('should have operationTypes property', () => {
      const operationTypesProperty = mongoDbChangeStream.description.properties.find(
        (prop: any) => prop.name === 'operationTypes'
      );
      expect(operationTypesProperty).toBeDefined();
      expect(operationTypesProperty?.type).toBe('multiOptions');
    });

    test('should support different watch levels', () => {
      const watchLevelProperty = mongoDbChangeStream.description.properties.find(
        (prop: any) => prop.name === 'watchLevel'
      );
      
      const watchLevels = watchLevelProperty?.options?.map((opt: any) => opt.value) || [];
      
      expect(watchLevels).toContain('collection');
      expect(watchLevels).toContain('database');
      expect(watchLevels).toContain('deployment');
    });

    test('should support MongoDB change stream operations', () => {
      const operationTypesProperty = mongoDbChangeStream.description.properties.find(
        (prop: any) => prop.name === 'operationTypes'
      );
      
      const operations = operationTypesProperty?.options?.map((opt: any) => opt.value) || [];
      
      expect(operations).toContain('insert');
      expect(operations).toContain('update');
      expect(operations).toContain('replace');
      expect(operations).toContain('delete');
    });
  });

  describe('Validation', () => {
    test('should have expected properties', () => {
      const properties = mongoDbChangeStream.description.properties;
      const propertyNames = properties.map((prop: any) => prop.name);
      
      // Should have core properties
      expect(propertyNames).toContain('watchLevel');
      expect(propertyNames).toContain('operationTypes');
      expect(properties.length).toBeGreaterThan(0);
    });
  });
});