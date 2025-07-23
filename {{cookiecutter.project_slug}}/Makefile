# Makefile for Survey Pipeline

# Variables
PYTHON := python3
PIP := pip
VENV := venv
PROJECT_NAME := {{ cookiecutter.project_slug }}
STREAMLIT_PORT := {{ cookiecutter.streamlit_port }}

.PHONY: help setup install clean test lint format run-dashboard run-pipeline docker-build docker-run

# Default target
help:
	@echo "🚀 {{ cookiecutter.project_name }} - Survey Pipeline"
	@echo ""
	@echo "Available commands:"
	@echo "  setup           - Set up development environment"
	@echo "  install         - Install dependencies"
	@echo "  clean           - Clean up temporary files"
	@echo "  test            - Run tests"
	@echo "  lint            - Run code linting"
	@echo "  format          - Format code with black"
	@echo "  run-dashboard   - Start Streamlit dashboard"
	@echo "  run-pipeline    - Execute complete pipeline"
	@echo "  run-prefect     - Start Prefect server"
	@echo "  docker-build    - Build Docker image"
	@echo "  docker-run      - Run with Docker Compose"
	@echo "  docker-dev      - Run development environment with Docker"

# Development setup
setup:
	@echo "🔧 Setting up development environment..."
	$(PYTHON) -m venv $(VENV)
	. $(VENV)/bin/activate && $(PIP) install --upgrade pip
	. $(VENV)/bin/activate && $(PIP) install -r requirements.txt
	@if [ ! -f .env ]; then cp .env.example .env; echo "⚠️  Please edit .env file"; fi
	@echo "✅ Setup complete! Run 'source venv/bin/activate' to activate environment"

# Install dependencies
install:
	$(PIP) install -r requirements.txt

# Clean temporary files
clean:
	@echo "🧹 Cleaning temporary files..."
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	rm -rf .pytest_cache
	rm -rf staging/raw/* staging/failed/* staging/cleaned/*
	@echo "✅ Cleanup complete"

# Run tests
test:
	@echo "🧪 Running tests..."
	pytest -v

# Code linting
lint:
	@echo "🔍 Running linters..."
	flake8 survey_pipeline/ streamlit_app/
	isort --check-only survey_pipeline/ streamlit_app/

# Code formatting
format:
	@echo "🎨 Formatting code..."
	black survey_pipeline/ streamlit_app/
	isort survey_pipeline/ streamlit_app/

# Run Streamlit dashboard
run-dashboard:
	@echo "📊 Starting Streamlit dashboard on port $(STREAMLIT_PORT)..."
	streamlit run streamlit_app/app.py --server.port $(STREAMLIT_PORT)

# Run complete pipeline
run-pipeline:
	@echo "🚀 Running complete pipeline..."
	$(PYTHON) -m survey_pipeline.cli run-pipeline

# Start Prefect server
run-prefect:
	@echo "⚡ Starting Prefect server..."
	prefect server start

# Test ODK connection
test-connection:
	@echo "🔌 Testing ODK Central connection..."
	$(PYTHON) -m survey_pipeline.cli test-connection

# Pipeline status
status:
	@echo "📈 Checking pipeline status..."
	$(PYTHON) -m survey_pipeline.cli status

# Docker commands
docker-build:
	@echo "🐳 Building Docker image..."
	docker build -t $(PROJECT_NAME) .

docker-run:
	@echo "🐳 Running with Docker Compose..."
	docker-compose up -d

docker-dev:
	@echo "🐳 Running development environment..."
	docker-compose -f docker-compose.yml -f docker-compose.dev.yml up

docker-stop:
	@echo "🛑 Stopping Docker containers..."
	docker-compose down

# Database/validation commands
init-expectations:
	@echo "📋 Initializing Great Expectations..."
	great_expectations init --no-view

validate-data:
	@echo "✅ Running data validation..."
	$(PYTHON) -m survey_pipeline.cli validate

# Data pipeline commands
ingest:
	@echo "📥 Ingesting data from ODK..."
	$(PYTHON) -m survey_pipeline.cli ingest

clean-data:
	@echo "🧹 Cleaning data..."
	$(PYTHON) -m survey_pipeline.cli clean

publish:
	@echo "📤 Publishing cleaned data..."
	$(PYTHON) -m survey_pipeline.cli publish

# Backup and rollback
backup:
	@echo "💾 Creating backup..."
	@timestamp=$$(date +%Y%m%d_%H%M%S); \
	tar -czf backup_$${timestamp}.tar.gz cleaned_stable/ validation_results/ logs/; \
	echo "✅ Backup created: backup_$${timestamp}.tar.gz"

rollback:
	@echo "🔄 Rolling back to previous version..."
	$(PYTHON) -m survey_pipeline.cli rollback --to $(DATE)

# Development helpers
install-dev:
	$(PIP) install -e .
	$(PIP) install pytest pytest-cov black flake8 isort

watch-logs:
	@echo "👀 Watching logs..."
	tail -f logs/*.log

# Production deployment
deploy:
	@echo "🚀 Deploying to production..."
	docker-compose -f docker-compose.yml --profile production up -d

# Quick start for new users
quickstart: setup test-connection run-dashboard

# Show project info
info:
	@echo "📋 Project Information"
	@echo "===================="
	@echo "Name: {{ cookiecutter.project_name }}"
	@echo "Client: {{ cookiecutter.client_name }}"
	@echo "Data Manager: {{ cookiecutter.data_manager_name }}"
	@echo "QA Officer: {{ cookiecutter.qa_officer_name }}"
	@echo "Dashboard Port: $(STREAMLIT_PORT)"
	@echo ""
	@echo "📁 Directory Status:"
	@ls -la raw/ staging/ cleaned_stable/ 2>/dev/null || echo "  (Directories will be created on first run)"
