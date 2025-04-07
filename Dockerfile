# Stage 1: Build the application
# Use an official Go image with Alpine Linux for smaller base
FROM golang:1.24.2-alpine AS builder

# Set the working directory inside the container
WORKDIR /app

# Copy go.mod and go.sum first to leverage Docker cache for dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the source code
COPY . .

# Build the application statically linked without CGO
# -ldflags="-w -s" strips debug symbols and reduces binary size
# -a installs the packages dependencies
RUN CGO_ENABLED=0 GOOS=linux go build -a -ldflags="-w -s" -o confluent-cloud-exporter .

# Stage 2: Create the final minimal image
# Use Alpine Linux for a small image size
FROM alpine:latest

# Set the working directory
WORKDIR /app

# Copy only the compiled binary from the builder stage
COPY --from=builder /app/confluent-cloud-exporter .

# Expose the port the exporter listens on (default 9184)
EXPOSE 9184

# Define the entrypoint for the container
ENTRYPOINT ["./confluent-cloud-exporter"]

# Default command-line arguments (can be overridden at runtime)
# Example: CMD ["-config.file=/config/config.yaml"]
# By default, it will run with flags/env vars or look for config.yaml in the working dir
CMD []