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
	"github.com/atomix/kubernetes-controller/pkg/controller/v1beta2/storage"
	"github.com/atomix/kubernetes-controller/pkg/controller/v1beta2/util/k8s"
	"github.com/atomix/raft-storage/pkg/apis/v1beta1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

var log = logf.Log.WithName("raft_controller")

// Add creates a new Partition ManagementGroup and adds it to the Manager. The Manager will set fields on the ManagementGroup
// and Start it when the Manager is Started.
func Add(mgr manager.Manager) error {
	log.Info("Add manager")
	reconciler := &Reconciler{
		client: mgr.GetClient(),
		scheme: mgr.GetScheme(),
	}
	gvk := schema.GroupVersionKind{
		Group:   v1beta1.RaftStorageClassGroup,
		Version: v1beta1.RaftStorageClassVersion,
		Kind:    v1beta1.RaftStorageClassKind,
	}
	return storage.AddClusterReconciler(mgr, reconciler, gvk)
}

var _ reconcile.Reconciler = &Reconciler{}

// Reconciler reconciles a Cluster object
type Reconciler struct {
	client client.Client
	scheme *runtime.Scheme
}

// Reconcile reads that state of the cluster for a Cluster object and makes changes based on the state read
// and what is in the Cluster.Spec
func (r *Reconciler) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	log.Info("Reconcile Cluster")
	cluster := &v1beta2.Cluster{}
	err := r.client.Get(context.TODO(), request.NamespacedName, cluster)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{Requeue: true}, err
	}

	storage := &v1beta1.RaftStorageClass{}
	name := types.NamespacedName{
		Namespace: cluster.Spec.Storage.Namespace,
		Name:      cluster.Spec.Storage.Name,
	}
	err = r.client.Get(context.TODO(), name, storage)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{Requeue: true}, err
	}

	err = r.reconcileConfigMap(cluster, storage)
	if err != nil {
		return reconcile.Result{}, err
	}

	err = r.reconcileStatefulSet(cluster, storage)
	if err != nil {
		return reconcile.Result{}, err
	}

	err = r.reconcileHeadlessService(cluster, storage)
	if err != nil {
		return reconcile.Result{}, err
	}

	err = r.reconcileStatus(cluster, storage)
	if err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{}, nil
}

func (r *Reconciler) reconcileConfigMap(cluster *v1beta2.Cluster, storage *v1beta1.RaftStorageClass) error {
	log.Info("Reconcile raft storage config map")
	cm := &corev1.ConfigMap{}
	name := types.NamespacedName{
		Namespace: cluster.Namespace,
		Name:      cluster.Name,
	}
	err := r.client.Get(context.TODO(), name, cm)
	if err != nil && k8serrors.IsNotFound(err) {
		err = r.addConfigMap(cluster, storage)
	}
	return err
}

func (r *Reconciler) reconcileStatefulSet(cluster *v1beta2.Cluster, storage *v1beta1.RaftStorageClass) error {
	log.Info("Reconcile raft storage stateful set")
	dep := &appsv1.Deployment{}
	name := types.NamespacedName{
		Namespace: cluster.Namespace,
		Name:      cluster.Name,
	}
	err := r.client.Get(context.TODO(), name, dep)
	if err != nil && k8serrors.IsNotFound(err) {
		err = r.addStatefulSet(cluster, storage)
	}
	return err
}

func (r *Reconciler) reconcileHeadlessService(cluster *v1beta2.Cluster, storage *v1beta1.RaftStorageClass) error {
	log.Info("Reconcile raft storage headless service")
	service := &corev1.Service{}
	name := types.NamespacedName{
		Namespace: cluster.Namespace,
		Name:      cluster.Name,
	}
	err := r.client.Get(context.TODO(), name, service)
	if err != nil && k8serrors.IsNotFound(err) {
		err = r.addHeadlessService(cluster, storage)
	}
	return err
}

func (r *Reconciler) reconcileStatus(cluster *v1beta2.Cluster, storage *v1beta1.RaftStorageClass) error {
	dep := &appsv1.Deployment{}
	name := types.NamespacedName{
		Namespace: cluster.Namespace,
		Name:      cluster.Name,
	}
	err := r.client.Get(context.TODO(), name, dep)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	if cluster.Status.ReadyPartitions < cluster.Spec.Partitions &&
		dep.Status.ReadyReplicas == dep.Status.Replicas {
		clusterID, err := k8s.GetClusterIDFromClusterAnnotations(cluster)
		if err != nil {
			return err
		}
		for partitionID := (cluster.Spec.Partitions * (clusterID - 1)) + 1; partitionID <= cluster.Spec.Partitions*clusterID; partitionID++ {
			partition := &v1beta2.Partition{}
			err := r.client.Get(context.TODO(), k8s.GetPartitionNamespacedName(cluster, partitionID), partition)
			if err != nil && !k8serrors.IsNotFound(err) {
				return err
			}
			if !partition.Status.Ready {
				partition.Status.Ready = true
				log.Info("Updating Partition status", "Name", partition.Name, "Namespace", partition.Namespace, "Ready", partition.Status.Ready)
				err = r.client.Status().Update(context.TODO(), partition)
				if err != nil {
					return err
				}
			}
		}

		// If we've made it this far, all partitions are ready. Update the cluster status
		cluster.Status.ReadyPartitions = cluster.Spec.Partitions
		log.Info("Updating Cluster status", "Name", cluster.Name, "Namespace", cluster.Namespace, "ReadyPartitions", cluster.Status.ReadyPartitions)
		return r.client.Status().Update(context.TODO(), cluster)
	}
	return nil
}
