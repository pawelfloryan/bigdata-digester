FROM golang:1.22.1-alpine

WORKDIR /app
EXPOSE 80

COPY go.mod go.sum ./
RUN go mod download

COPY . ./

RUN go build -o digester main.go

ENTRYPOINT ["./digester"]