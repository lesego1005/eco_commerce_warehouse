# Use the same Airflow version you're currently running
FROM apache/airflow:2.9.3

# Switch to root to install system-level dependencies
USER root

# Install PostgreSQL client (for psql) and build tools for Python packages
RUN apt-get update && apt-get install -y \
    postgresql-client \
    libpq-dev \
    gcc \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user for Python installations (security best practice)
USER airflow

# Copy your requirements file into the image
COPY requirements.txt .

# Install all packages (including pandas, scikit-learn for AI, and streamlit)
RUN pip install --no-cache-dir -r requirements.txt

# Ensure the app code is copied (optional, depending on your docker-compose volumes)
# COPY . .