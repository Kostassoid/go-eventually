module github.com/get-eventually/go-eventually/eventstore/postgres

go 1.15

replace github.com/get-eventually/go-eventually => ../..

require (
	github.com/containerd/containerd v1.5.5 // indirect
	github.com/docker/docker v20.10.8+incompatible // indirect
	github.com/docker/go-connections v0.4.0 // indirect
	github.com/get-eventually/go-eventually v0.0.0-20210513211936-20a5f17a39fa
	github.com/golang-migrate/migrate v3.5.4+incompatible
	github.com/gorilla/mux v1.8.0 // indirect
	github.com/lib/pq v1.10.1 // indirect
	github.com/moby/term v0.0.0-20210619224110-3f7ff695adc6 // indirect
	github.com/morikuni/aec v1.0.0 // indirect
	github.com/stretchr/testify v1.7.0
	go.uber.org/zap v1.18.1
	golang.org/x/time v0.0.0-20210723032227-1f47c861a9ac // indirect
	google.golang.org/grpc v1.40.0 // indirect
)
