FROM python:3.11-slim as builder

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt

# Final stage
FROM python:3.11-slim

# Create non-root user
RUN useradd -m -u 1000 appuser && \
    mkdir -p /app && \
    chown -R appuser:appuser /app

WORKDIR /app

# Copy dependencies from builder
COPY --from=builder /root/.local /home/appuser/.local
COPY --chown=appuser:appuser worker.py .

# Set PATH for user packages
ENV PATH=/home/appuser/.local/bin:$PATH

# Switch to non-root user
USER appuser

# Expose metrics port
EXPOSE 8001

# Health check (check if process is running)
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD pgrep -f worker.py || exit 1

# Run the worker
CMD ["python", "-u", "worker.py"]