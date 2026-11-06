FROM golang:1.27-alpine AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/vsrd ./cmd/vsrd \
 && mkdir /out/data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/vsrd /usr/local/bin/vsrd
COPY --from=build --chown=65532:65532 /out/data /data
USER 65532:65532
EXPOSE 10188
ENTRYPOINT ["vsrd"]
