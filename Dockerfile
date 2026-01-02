# Build stage
FROM golang:1.21-alpine AS builder

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the application with CGO enabled for SQLite
RUN CGO_ENABLED=1 GOOS=linux go build -a -installsuffix cgo -o uk-rail-schedule-api .

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

# Set entrypoint
CMD ["/app/start.sh"]