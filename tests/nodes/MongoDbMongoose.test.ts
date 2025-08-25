import { MongoDbMongoose } from '../../nodes/MongoDbMongoose/MongoDbMongoose.node';

describe('MongoDbMongoose Node', () => {
  let mongoDbMongoose: MongoDbMongoose;

  beforeEach(() => {
    mongoDbMongoose = new MongoDbMongoose();
  });

  describe('Node Description', () => {
    test('should have correct display name', () => {
      expect(mongoDbMongoose.description.displayName).toBe('MongoDB Mongoose');
    });

    test('should have correct node name', () => {
      expect(mongoDbMongoose.description.name).toBe('mongoDbMongoose');
    });

    test('should have transform group', () => {
      expect(mongoDbMongoose.description.group).toContain('transform');
    });

    test('should have correct icon', () => {
      expect(mongoDbMongoose.description.icon).toBe('file:mongodb.svg');
    });

    test('should have main input connection', () => {
      expect(mongoDbMongoose.description.inputs).toEqual(['main']);
    });

    test('should have main output connection', () => {
      expect(mongoDbMongoose.description.outputs).toEqual(['main']);
    });
  });

  describe('Node Properties', () => {
    test('should have operation property', () => {
      const operationProperty = mongoDbMongoose.description.properties.find(
        prop => prop.name === 'operation'
      );
      expect(operationProperty).toBeDefined();
      expect(operationProperty?.type).toBe('options');
    });

    test('should have collection property', () => {
      const collectionProperty = mongoDbMongoose.description.properties.find(
        prop => prop.name === 'collection'
      );
      expect(collectionProperty).toBeDefined();
      expect(collectionProperty?.type).toBe('string');
    });

    test('should support CRUD operations', () => {
      const operationProperty = mongoDbMongoose.description.properties.find(
        prop => prop.name === 'operation'
      );
      
      const operations = operationProperty?.options?.map((opt: any) => opt.value) || [];
      
      expect(operations).toContain('create');
      expect(operations).toContain('find');
      expect(operations).toContain('findOne');
      expect(operations).toContain('update');
      expect(operations).toContain('delete');
      expect(operations).toContain('aggregate');
      expect(operations).toContain('count');
    });
  });

  describe('Validation', () => {
    test('should validate required collection name', () => {
      const collectionProperty = mongoDbMongoose.description.properties.find(
        prop => prop.name === 'collection'
      );
      expect(collectionProperty?.required).toBe(true);
    });
  });
});
