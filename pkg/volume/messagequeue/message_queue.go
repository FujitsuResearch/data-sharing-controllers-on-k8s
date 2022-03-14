// Copyright (c) 2022 Fujitsu Limited

package messagequeue

import (
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/messagequeue"
)

func GetMessageQueueTopic(
	pvcNamespace string, pvcName string) *messagequeue.MessageQueueTopic {
	elements := []string{
		pvcNamespace,
		pvcName,
	}

	return messagequeue.NewMessageQueueFileSystemTopic(elements)
}

func GetGroupId(persistentVolumeClaimRef *corev1.ObjectReference) string {
	elements := []string{
		persistentVolumeClaimRef.Namespace,
		persistentVolumeClaimRef.Name,
		time.Now().String(),
	}

	return messagequeue.GetGroupId(elements)
}
