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

package k8s

import (
	corev1 "k8s.io/api/core/v1"
)

const (
	appKey      = "app"
	atomixApp   = "redis"
	typeKey     = "type"
	databaseKey = "database"
	clusterKey  = "cluster"
)

const (
	controllerAnnotation = "cloud.atomix.io/controller"
	typeAnnotation       = "cloud.atomix.io/type"
	databaseAnnotation   = "cloud.atomix.io/group"
	clusterAnnotation    = "cloud.atomix.io/cluster"
)

const (
	clusterType = "cluster"
	proxyType   = "proxy"
)

const (
	headlessServiceSuffix  = "hs"
	disruptionBudgetSuffix = "pdb"
	configSuffix           = "config"
)

const (
	configPath         = "/etc/atomix"
	clusterConfigFile  = "cluster.json"
	protocolConfigFile = "protocol.json"
	dataPath           = "/var/lib/atomix"
)

const (
	configVolume = "config"
	dataVolume   = "data"
)

const (
	controllerNameVar      = "CONTROLLER_NAME"
	controllerNamespaceVar = "CONTROLLER_NAMESPACE"
)

// newContainer returns a container for a node
func newContainer(container Container) corev1.Container {
	container.env = append(container.env, corev1.EnvVar{
		Name: "NODE_ID",
		ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{
				FieldPath: "metadata.name",
			},
		},
	})

	return corev1.Container{
		Name:            container.Name(),
		Image:           container.Image(),
		ImagePullPolicy: container.PullPolicy(),
		Env:             container.Env(),
		Resources:       container.Resources(),
		Ports:           container.Ports(),
		Args:            container.args,
		ReadinessProbe:  container.ReadinessProbe(),
		LivenessProbe:   container.LivenessProbe(),
		VolumeMounts:    container.VolumeMounts(),
	}
}

// newDataVolumeMount returns a data volume mount for a pod
func newDataVolumeMount() corev1.VolumeMount {
	return corev1.VolumeMount{
		Name:      dataVolume,
		MountPath: dataPath,
	}
}

// newConfigVolumeMount returns a configuration volume mount for a pod
func newConfigVolumeMount() corev1.VolumeMount {
	return corev1.VolumeMount{
		Name:      configVolume,
		MountPath: configPath,
	}
}

// newConfigVolume returns the configuration volume for a pod
func newConfigVolume(name string) corev1.Volume {
	return corev1.Volume{
		Name: configVolume,
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: name,
				},
			},
		},
	}
}

// newDataVolume returns the data volume for a pod
func newDataVolume() corev1.Volume {
	return corev1.Volume{
		Name: dataVolume,
		VolumeSource: corev1.VolumeSource{
			EmptyDir: &corev1.EmptyDirVolumeSource{},
		},
	}
}
