// Copyright (c) 2022 Fujitsu Limited

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

/* [REF] scheduledworkflow in kubeflow pipelines */

type DeletePolicy string

const (
	DefaultPolicy DeletePolicy = ""
	ChmodPolicy   DeletePolicy = "chmod"
	RemovePolicy  DeletePolicy = "rm"
	NotifyPolicy  DeletePolicy = "notify"
)

//type CronSchedule struct {
//	// Time at which scheduling starts in RFC 3339 format.
//	// If no start time is specified, the StartTime is the creation time of the schedule.
//	// +optional
//	StartTime string `json:"startTime,omitempty"`
//
//	// Time at which scheduling ends in RFC 3339 format.
//	// If no end time is specified, the EndTime is the end of time.
//	// +optional
//	EndTime string `json:"endTime,omitempty"`
//
//	// Cron string describing when a workflow should be created within the
//	// time interval defined by StartTime and EndTime.
//	// +optional
//	Cron string `json:"cron,omitempty"`
//}
//
//type PeriodicSchedule struct {
//	// Time at which scheduling starts in RFC 3339 format.
//	// If no start time is specified, the StartTime is the creation time of the schedule.
//	// +optional
//	StartTime string `json:"startTime,omitempty"`
//
//	// Time at which scheduling ends in RFC 3339 format.
//	// If no end time is specified, the EndTime is the end of time.
//	// +optional
//	EndTime string `json:"endTime,omitempty"`
//
//	// Cron string describing when a workflow should be created within the
//	// time interval defined by StartTime and EndTime.
//	// +optional
//	Interval string `json:"interval,omitempty"`
//}

type Trigger struct {
	//	// Create data liftimer according to a cron schedule.
	//	CronSchedule *CronSchedule `json:"cronSchedule,omitempty"`
	//
	//	// Create data lifetimer periodically.
	//	PeriodicSchedule *PeriodicSchedule `json:"periodicSchedule,omitempty"`

	// Time at which scheduling starts in RFC 3339 format.
	// If no start time is specified, the StartTime is the creation time of the schedule.
	// +optional
	StartTime string `json:"startTime,omitempty"`

	// Time at which scheduling ends in RFC 3339 format.
	// If no end time is specified, the EndTime is the end of time.
	// +optional
	EndTime string `json:"endTime"`
}

type MessageQueueSpec struct {
	MessageQueueRef *corev1.ObjectReference `json:"messageQueueRef"`
	MaxBatchBytes   int                     `json:"maxBatchBytes,omitempty"`
}

type PublishMessageQueueSpec struct {
	*MessageQueueSpec
	UpdatePublishChannelBufferSize int `json:"updatePublishChannelBufferSize,omitempty"`
}

type AllowedScript struct {
	AbsolutePath              string `json:"absolutePath"`
	Checksum                  string `json:"checksum,omitempty"`
	Writable                  bool   `json:"writable,omitempty"`
	RelativePathToInitWorkDir string `json:"relativePathToInitWorkDir,omitempty"`
}

type AllowedExecutable struct {
	CmdAbsolutePath string           `json:"cmdAbsolutePath"`
	Scripts         []*AllowedScript `json:"scripts,omitempty"`
	Checksum        string           `json:"checksum,omitempty"`
	Writable        bool             `json:"writable,omitempty"`
}

type InputFileSystemSpec struct {
	FromPersistentVolumeClaimRef *corev1.ObjectReference `json:"fromPersistentVolumeClaimRef"`
	ToPersistentVolumeClaimRef   *corev1.ObjectReference `json:"toPersistentVolumeClaimRef"`
	AllowedExecutables           []*AllowedExecutable    `json:"allowedExecutables,omitempty"`
}

type InputRdbSpec struct {
	SourceDb         string                  `json:"sourceDb"`
	LocalDb          string                  `json:"localDb"`
	ConfigurationRef *corev1.ObjectReference `json:"configurationRef"`
}

type InputDataSpec struct {
	MessageQueueSubscriber *MessageQueueSpec    `json:"messageQueueSubscriber"`
	FileSystemSpec         *InputFileSystemSpec `json:"fileSystemSpec,omitempty"`
	RdbSpec                *InputRdbSpec        `json:"rdbSpec,omitempty"`
}

type OutputFileSystemSpec struct {
	PersistentVolumeClaimRef *corev1.ObjectReference `json:"persistentVolumeClaimRef"`
	AllowedExecutables       []*AllowedExecutable    `json:"allowedExecutables,omitempty"`
}

type OutputRdbSpec struct {
	DestinationDb    string                  `json:"destinationDb"`
	ConfigurationRef *corev1.ObjectReference `json:"configurationRef"`
}

type OutputDataSpec struct {
	MessageQueuePublisher *PublishMessageQueueSpec `json:"messageQueuePublisher,omitempty"`
	FileSystemSpec        *OutputFileSystemSpec    `json:"fileSystemSpec,omitempty"`
	RdbSpec               *OutputRdbSpec           `json:"rdbSpec,omitempty"`
}

type LifetimeSpec struct {
	// +optional
	Trigger *Trigger `json:"trigger"`

	InputData  []InputDataSpec  `json:"inputData"`
	OutputData []OutputDataSpec `json:"outputData,omitempty"`

	DeletePolicy DeletePolicy `json:"deletePolicy,omitempty"`
}

type ProviderStatus struct {
	LastUpdatedAt metav1.Time `json:"lastUpdatedAt"`
}

type ConsumerStatus struct {
	LastUpdatedAt metav1.Time `json:"lastUpdatedAt"`
	ExpiredAt     metav1.Time `json:"expiredAt"`
}

type LifetimeStatus struct {
	Provider ProviderStatus            `json:"provider,ommitempty"`
	Consumer map[string]ConsumerStatus `json:"consumer,ommitempty"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type DataLifetime struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   LifetimeSpec   `json:"spec,omitempty"`
	Status LifetimeStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type DataLifetimeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []DataLifetime `json:"items"`
}
