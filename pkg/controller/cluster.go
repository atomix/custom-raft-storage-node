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

package controller

import (
	"context"

	"github.com/atomix/kubernetes-controller/pkg/apis/cloud/v1beta2"
	"github.com/atomix/raft-storage/pkg/apis/v1beta1"
	"github.com/atomix/raft-storage/pkg/controller/util/k8s"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func (r *Reconciler) addHeadlessService(cluster *v1beta2.Cluster, storage *v1beta1.RaftStorageClass) error {
	log.Info("Creating headless raft service", "Name", cluster.Name, "Namespace", cluster.Namespace)
	service := k8s.NewClusterHeadlessService(cluster)
	if err := controllerutil.SetControllerReference(storage, service, r.scheme); err != nil {
		return err
	}
	return r.client.Create(context.TODO(), service)
}

func (r *Reconciler) addStatefulSet(cluster *v1beta2.Cluster, storage *v1beta1.RaftStorageClass) error {
	log.Info("Creating raft replicas", "Name", cluster.Name, "Namespace", cluster.Namespace)
	set, err := k8s.NewBackendStatefulSet(cluster, storage)
	if err != nil {
		return err
	}
	if err := controllerutil.SetControllerReference(storage, set, r.scheme); err != nil {
		return err
	}

	return r.client.Create(context.TODO(), set)
}

func (r *Reconciler) addConfigMap(cluster *v1beta2.Cluster, storage *v1beta1.RaftStorageClass) error {
	log.Info("Creating raft ConfigMap", "Name", cluster.Name, "Namespace", cluster.Namespace)
	var config interface{}

	cm, err := k8s.NewClusterConfigMap(cluster, storage, config)
	if err != nil {
		return err
	}
	if err := controllerutil.SetControllerReference(storage, cm, r.scheme); err != nil {
		return err
	}
	return r.client.Create(context.TODO(), cm)
}
