module github.com/atomix/raft-storage

go 1.12

require (
	github.com/atomix/api v0.1.0
	github.com/atomix/go-framework v0.1.0
	github.com/atomix/kubernetes-controller v0.2.0-beta.1
	github.com/atomix/raft-replica v0.0.0-20200124061410-f429149bc81b
	github.com/gogo/protobuf v1.3.1
	github.com/golang/mock v1.3.1
	github.com/golang/protobuf v1.3.2
	github.com/sirupsen/logrus v1.4.2
	github.com/stretchr/testify v1.4.0
	google.golang.org/grpc v1.27.0
	k8s.io/api v0.17.2
	k8s.io/apimachinery v0.17.2
	k8s.io/client-go v0.17.2
	sigs.k8s.io/controller-runtime v0.5.2
)
