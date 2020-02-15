FROM golang:1.13-alpine AS builder

WORKDIR /app
RUN apk --no-cache add ca-certificates
ADD go.mod .
ADD go.sum .
RUN go mod download
ADD . .
RUN CGO_ENABLED=0 go build -a -installsuffix cgo -o gompress .

FROM scratch

COPY --from=builder /app/gompress /gompress
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
ENTRYPOINT ["/gompress"]
