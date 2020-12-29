module github.com/atomix/go-framework

go 1.13

require (
	github.com/atomix/api v0.3.3
	github.com/atomix/api/go v0.3.3
	github.com/gogo/protobuf v1.3.1
	github.com/golang/protobuf v1.4.1
	github.com/google/uuid v1.1.2
	github.com/sirupsen/logrus v1.4.2
	github.com/stretchr/testify v1.4.0
	google.golang.org/grpc v1.33.2
)

replace github.com/atomix/api/go => ../atomix-api/go
