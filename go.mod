module github.com/atomix/go-framework

go 1.12

require (
	github.com/atomix/api v0.0.0-20200206211058-f075fb5b6d1b
	github.com/atomix/atomix-go-node v0.0.0-20200114212450-178a2dc70336
	github.com/atomix/go-client v0.0.0-20200206051325-cdc03bd1c8bc
	github.com/atomix/go-local v0.0.0-20200207202057-4a81cbdd3325
	github.com/gogo/protobuf v1.3.1
	github.com/golang/protobuf v1.3.2
	github.com/sirupsen/logrus v1.4.2
	github.com/stretchr/testify v1.4.0
	google.golang.org/grpc v1.27.0
)

replace github.com/atomix/api => ../api

replace github.com/atomix/go-client => ../go-client
