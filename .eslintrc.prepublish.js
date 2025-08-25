module.exports = {
  root: true,
  env: {
    browser: true,
    es6: true,
    node: true,
  },
  parser: '@typescript-eslint/parser',
  parserOptions: {
    project: 'tsconfig.json',
    sourceType: 'module',
  },
  plugins: ['n8n-nodes-base'],
  extends: ['plugin:n8n-nodes-base/community'],
  rules: {
    'n8n-nodes-base/community-package-json-name-still-default': 'error',
    'n8n-nodes-base/community-package-json-author-email-still-default': 'error',
    'n8n-nodes-base/community-package-json-author-name-still-default': 'error',
  },
};
