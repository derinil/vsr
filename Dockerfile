FROM golang:1.27-alpine AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/vsrd ./cmd/vsrmon \
 && mkdir /out/data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/vsrd /out/vsrmon /usr/local/bin/
COPY --from=build --chown=65532:65532 /out/data /data
USER 65532:65532
EXPOSE 10188 8080
ENTRYPOINT ["vsrd"]
