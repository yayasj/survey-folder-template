#!/bin/bash
# Setup script for new survey project

echo "🚀 Setting up {{ cookiecutter.project_name }}..."

# Create Python virtual environment
echo "Creating virtual environment..."
python3 -m venv venv
source venv/bin/activate

# Install dependencies
echo "Installing dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

# Copy environment template
echo "Setting up environment variables..."
if [ ! -f .env ]; then
    cp .env.example .env
    echo "⚠️  Please edit .env file with your ODK Central credentials"
else
    echo "✅ .env file already exists"
fi

# Initialize Great Expectations
echo "Initializing Great Expectations..."
if [ ! -d .great_expectations ]; then
    great_expectations init --no-view
    echo "✅ Great Expectations initialized"
else
    echo "✅ Great Expectations already initialized"
fi

# Create Excel version of cleaning rules
echo "Setting up cleaning rules..."
if [ ! -f cleaning_rules.xlsx ]; then
    python -c "
import pandas as pd
df = pd.read_csv('cleaning_rules.csv')
df.to_excel('cleaning_rules.xlsx', index=False)
print('✅ Created cleaning_rules.xlsx from CSV template')
"
else
    echo "✅ cleaning_rules.xlsx already exists"
fi

# Set up Prefect
echo "Configuring Prefect..."
prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"

echo ""
echo "🎉 Setup complete!"
echo ""
echo "Next steps:"
echo "1. Edit .env file with your ODK Central credentials"
echo "2. Update config.yml with your survey details"
echo "3. Run: source venv/bin/activate"
echo "4. Test connection: python -c 'from survey_pipeline.cli import test_connection; test_connection()'"
echo "5. Start dashboard: streamlit run streamlit_app/app.py"
echo ""
