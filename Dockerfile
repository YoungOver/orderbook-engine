FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/obe ./cmd/server

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/obe /obe
VOLUME /data
EXPOSE 8080
ENTRYPOINT ["/obe", "-addr", ":8080", "-journal", "/data/journal.bin"]
