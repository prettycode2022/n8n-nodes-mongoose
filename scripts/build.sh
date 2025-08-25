#!/bin/bash

# Script to build n8n-nodes-mongodb-mongoose

set -e

echo "🔨 Building n8n-nodes-mongodb-mongoose..."

# Check if we're in the right directory
if [ ! -f "package.json" ]; then
    echo "❌ Error: package.json not found. Please run this script from the project root."
    exit 1
fi

# Clean previous build
if [ -d "dist" ]; then
    echo "🧹 Cleaning previous build..."
    rm -rf dist
fi

# Install dependencies if node_modules doesn't exist
if [ ! -d "node_modules" ]; then
    echo "📦 Installing dependencies..."
    npm install
fi

# Compile TypeScript
echo "📝 Compiling TypeScript..."
npx tsc

# Copy icons and other assets
echo "🎨 Copying assets..."
npx gulp build:icons

# Verify build
if [ -d "dist" ]; then
    echo "✅ Build completed successfully!"
    echo "📁 Build output:"
    find dist -type f -name "*.js" -o -name "*.svg" -o -name "*.png" | head -10
else
    echo "❌ Build failed - dist directory not found."
    exit 1
fi
