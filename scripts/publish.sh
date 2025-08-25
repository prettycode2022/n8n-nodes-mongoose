#!/bin/bash

# Script to build and publish n8n-nodes-mongodb-mongoose to npmjs

set -e

echo "🚀 Starting build and publish process for n8n-nodes-mongodb-mongoose..."

# Check if we're in the right directory
if [ ! -f "package.json" ]; then
    echo "❌ Error: package.json not found. Please run this script from the project root."
    exit 1
fi

# Check if user is logged in to npm
if ! npm whoami > /dev/null 2>&1; then
    echo "❌ Error: You are not logged in to npm. Please run 'npm login' first."
    exit 1
fi

# Install dependencies
echo "📦 Installing dependencies..."
npm install

# Run linting
echo "🔍 Running linting..."
npm run lint

# Build the project
echo "🔨 Building project..."
npm run build

# Check if dist directory exists
if [ ! -d "dist" ]; then
    echo "❌ Error: Build failed - dist directory not found."
    exit 1
fi

# Run pre-publish linting
echo "🔍 Running pre-publish linting..."
npm run prepublishOnly

# Show package info
echo "📋 Package information:"
npm pack --dry-run

# Confirm publication
echo ""
echo "🤔 Do you want to publish this package to npmjs? (y/N)"
read -r response

if [[ "$response" =~ ^([yY][eE][sS]|[yY])$ ]]; then
    echo "📤 Publishing to npmjs..."
    npm publish
    
    if [ $? -eq 0 ]; then
        echo "✅ Successfully published n8n-nodes-mongodb-mongoose to npmjs!"
        echo ""
        echo "📖 Your package is now available at:"
        echo "   https://www.npmjs.com/package/n8n-nodes-mongodb-mongoose"
        echo ""
        echo "🔧 To install in n8n:"
        echo "   npm install n8n-nodes-mongodb-mongoose"
        echo ""
        echo "📚 Don't forget to:"
        echo "   1. Update your README with installation instructions"
        echo "   2. Add examples and documentation"
        echo "   3. Consider submitting for n8n community verification"
    else
        echo "❌ Publication failed!"
        exit 1
    fi
else
    echo "❌ Publication cancelled."
    exit 0
fi
