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

package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RaftStorageClassGroup raft storage class group
const RaftStorageClassGroup = "storage.cloud.atomix.io"

// RaftStorageClassVersion raft storage class version
const RaftStorageClassVersion = "v1beta1"

// RaftStorageClassKind raft storage class kind
const RaftStorageClassKind = "RaftStorageClass"

// RaftStorageClassSpec defines the desired state of RaftStorageClass
type RaftStorageClassSpec struct {
}

// RaftStorageClassStatus defines the observed state of RaftStorageClass
type RaftStorageClassStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file
}

// +kubebuilder:object:root=true

// RaftStorageClass is the Schema for the raftstorageclasses API
type RaftStorageClass struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RaftStorageClassSpec   `json:"spec,omitempty"`
	Status RaftStorageClassStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// RaftStorageClassList contains a list of RaftStorageClass
type RaftStorageClassList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RaftStorageClass `json:"items"`
}

func init() {
	SchemeBuilder.Register(&RaftStorageClass{}, &RaftStorageClassList{})
}
