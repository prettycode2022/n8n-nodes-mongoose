// Test setup file

// Extend timeout for MongoDB operations
jest.setTimeout(30000);

// Mock n8n workflow if needed
jest.mock('n8n-workflow', () => ({
  NodeOperationError: class extends Error {
    constructor(node: any, message: string) {
      super(message);
      this.name = 'NodeOperationError';
    }
  },
  NodeConnectionType: {
    Main: 'main'
  }
}));

// Global test setup
beforeAll(async () => {
  // Setup global test resources if needed
});

afterAll(async () => {
  // Cleanup global test resources
});
