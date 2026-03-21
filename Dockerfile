# Build Stage
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Instalar dependencias del sistema necesarias para cgo (si fuera necesario, aunque aquí usaremos CGO_ENABLED=0 pero git a veces ayuda)
RUN apk add --no-cache git

# Copiar archivos de dependencias
COPY go.mod go.sum ./
RUN go mod download

# Copiar el código fuente
COPY . .

# Compilar Syncer
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/bin/syncer ./cmd/syncer

# Final Stage
FROM alpine:3.19

WORKDIR /app

# Instalar certificados CA para https (importante para conectar a AWS/Azure o APIs externas si fuera el caso)
RUN apk --no-cache add ca-certificates tzdata

# Copiar binarios del builder
COPY --from=builder /app/bin/syncer /app/syncer

RUN adduser -D nonroot
USER nonroot
