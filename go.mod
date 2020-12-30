module github.com/atomix/go-framework

go 1.13

require (
	github.com/atomix/api/go v0.3.3
	github.com/gogo/protobuf v1.3.1
	github.com/golang/protobuf v1.4.1
	github.com/google/uuid v1.1.2
	github.com/mitchellh/go-homedir v1.1.0
	github.com/spf13/viper v1.7.1
	github.com/stretchr/testify v1.4.0
	go.uber.org/zap v1.16.0
	google.golang.org/grpc v1.33.2
	gopkg.in/yaml.v2 v2.2.4
)

replace github.com/atomix/api/go => ../atomix-api/go
