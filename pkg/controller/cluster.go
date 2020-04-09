// Copyright 2020-present Open Networking Foundation.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package test

import (
	"github.com/atomix/kubernetes-controller/pkg/apis/cloud/v1beta2"
	"github.com/atomix/raft-storage/pkg/apis/v1beta1"
)

func (r *Reconciler) addHeadlessService(cluster *v1beta2.Cluster, storage *v1beta1.RaftStorageClass) error {
	panic("Implement me")
}

func (r *Reconciler) addStatefulSet(cluster *v1beta2.Cluster, storage *v1beta1.RaftStorageClass) error {
	panic("Implement me")
}

func (r *Reconciler) addConfigMap(cluster *v1beta2.Cluster, storage *v1beta1.RaftStorageClass) error {
	panic("Implement me")
}
