#!/bin/bash
# Migration script to copy dashboard from data_feeder to rust_bot
# Run this on your local machine

set -e

echo "🚀 Dashboard Migration Script"
echo "=============================="
echo ""

# Set paths
DATA_FEEDER_PATH="/Users/rafael.siqueira/dev/personal/data_feeder"
RUST_BOT_PATH="/Users/rafael.siqueira/dev/personal/rust_bot"

# Check if directories exist
if [ ! -d "$DATA_FEEDER_PATH" ]; then
    echo "❌ Error: data_feeder directory not found at $DATA_FEEDER_PATH"
    exit 1
fi

if [ ! -d "$RUST_BOT_PATH" ]; then
    echo "❌ Error: rust_bot directory not found at $RUST_BOT_PATH"
    echo "Please update RUST_BOT_PATH in this script"
    exit 1
fi

echo "📂 Source: $DATA_FEEDER_PATH"
echo "📂 Target: $RUST_BOT_PATH"
echo ""

# Copy dashboard
echo "📦 Copying dashboard..."
if [ -d "$RUST_BOT_PATH/dashboard" ]; then
    echo "⚠️  Warning: dashboard directory already exists in rust_bot"
    read -p "Do you want to overwrite it? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "❌ Aborted"
        exit 1
    fi
    rm -rf "$RUST_BOT_PATH/dashboard"
fi

cp -r "$DATA_FEEDER_PATH/dashboard" "$RUST_BOT_PATH/"
echo "✅ Dashboard copied"

# Copy API documentation
echo "📄 Copying API documentation..."
cp "$DATA_FEEDER_PATH/API_ENDPOINTS.md" "$RUST_BOT_PATH/" 2>/dev/null || echo "⚠️  API_ENDPOINTS.md not found"
cp "$DATA_FEEDER_PATH/API_QUICK_REFERENCE.md" "$RUST_BOT_PATH/" 2>/dev/null || echo "⚠️  API_QUICK_REFERENCE.md not found"
cp "$DATA_FEEDER_PATH/API_ANALYSIS_SUMMARY.txt" "$RUST_BOT_PATH/" 2>/dev/null || echo "⚠️  API_ANALYSIS_SUMMARY.txt not found"
echo "✅ Documentation copied"

# Update .env
echo "🔧 Setting up .env..."
if [ -f "$RUST_BOT_PATH/dashboard/.env.example" ]; then
    if [ ! -f "$RUST_BOT_PATH/dashboard/.env" ]; then
        cp "$RUST_BOT_PATH/dashboard/.env.example" "$RUST_BOT_PATH/dashboard/.env"
        # Update API base URL to rust_bot default (9876)
        sed -i '' 's/localhost:8080/localhost:9876/g' "$RUST_BOT_PATH/dashboard/.env" 2>/dev/null || \
        sed -i 's/localhost:8080/localhost:9876/g' "$RUST_BOT_PATH/dashboard/.env"
        echo "✅ .env file created with API_BASE_URL=http://localhost:9876"
    else
        echo "ℹ️  .env already exists, skipping"
    fi
fi

echo ""
echo "🎉 Migration complete!"
echo ""
echo "Next steps:"
echo "1. cd $RUST_BOT_PATH/dashboard"
echo "2. npm install"
echo "3. npm run dev"
echo ""
echo "📖 See MIGRATION_INSTRUCTIONS.md for detailed integration steps"
