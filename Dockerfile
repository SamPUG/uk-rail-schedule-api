# Build stage
FROM golang:1.21-alpine AS builder

WORKDIR /app

# Install build dependencies for CGO/SQLite
RUN apk add --no-cache gcc musl-dev sqlite-dev

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the application (musl doesn't have pread64/pwrite64, so disable them)
RUN CGO_CFLAGS="-D_LARGEFILE64_SOURCE" go build -o uk-rail-schedule-api .

# Runtime stage
FROM alpine:latest

# Install runtime dependencies
RUN apk add --no-cache \
    sqlite \
    curl \
    ca-certificates \
    tzdata \
    busybox

# Set timezone to London
ENV TZ=Europe/London

# Create app directory
WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/uk-rail-schedule-api .

# Copy config file
COPY config.yaml .

# Create data directory
RUN mkdir data

# Copy update script
COPY update-schedule-feed.sh .
RUN chmod +x update-schedule-feed.sh

# Set up cron job to run update script daily at 2 AM
RUN echo "0 2 * * * /app/update-schedule-feed.sh" > /etc/crontabs/root

# Copy startup script
COPY start.sh .
RUN chmod +x start.sh

# Expose port
EXPOSE 3333

# Ensure we're in the app directory when running
WORKDIR /app

# Set entrypoint
CMD ["/app/start.sh"]