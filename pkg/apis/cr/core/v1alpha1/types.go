// Copyright (c) 2022 Fujitsu Limited

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type DataStatus struct {
	CreatedAt     metav1.Time `json:"createAt"`
	LastUpdatedAt metav1.Time `json:"lastUpdatedAt"`
}

type MessageQueueSpec struct {
	Brokers []string                `json:"brokers"`
	SaslRef *corev1.ObjectReference `json:"saslRef"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type MessageQueue struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   MessageQueueSpec `json:"spec,omitempty"`
	Status DataStatus       `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type MessageQueueList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []MessageQueue `json:"items"`
}

type RdbSpec struct {
	Url               string                  `json:"url"`
	AuthenticationRef *corev1.ObjectReference `json:"authenticationRef"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type RdbData struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RdbSpec    `json:"spec,omitempty"`
	Status DataStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type RdbDataList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []RdbData `json:"items"`
}
