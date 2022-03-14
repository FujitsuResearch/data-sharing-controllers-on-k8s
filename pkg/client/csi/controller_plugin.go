// Copyright (c) 2022 Fujitsu Limited

package csi

import (
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/util"
)

const (
	UsageControlKey        = "csi.volumes.dsc.io/usagecontrol"
	MessageQueuePublishKey = "csi.volumes.dsc.io/mqpublish"
)

type CsiControllerClient struct {
	kubeClient kubernetes.Interface
}

func NewCsiControllerClient(
	kubeApiServerUrl string, kubeConfigPath string) (
	*CsiControllerClient, error) {
	kubeConfig, err := clientcmd.BuildConfigFromFlags(
		kubeApiServerUrl, kubeConfigPath)
	if err != nil {
		return nil, fmt.Errorf("Error building kubeconfig: %v", err.Error())
	}

	kubeClient, err := kubernetes.NewForConfig(kubeConfig)
	if err != nil {
		return nil, fmt.Errorf(
			"Error building kubernetes clientset: %v", err.Error())
	}

	return &CsiControllerClient{
		kubeClient: kubeClient,
	}, nil
}

func (ccc *CsiControllerClient) AreUsageControlAndDataExportEnabled(
	pvcNamespace string, pvcName string) (bool, bool, error) {
	ctx, cancel := util.GetTimeoutContext(util.DefaultKubeClientTimeout)
	defer cancel()

	pvc, err := ccc.kubeClient.CoreV1().PersistentVolumeClaims(
		pvcNamespace).Get(ctx, pvcName, metav1.GetOptions{})
	if err != nil {
		return false, false, fmt.Errorf(
			"Not get PVC[%s:%s]: %v", pvcNamespace, pvcName, err.Error())
	}

	usageControlEnabled := false
	usageControl := pvc.Labels[UsageControlKey]
	if usageControl == "true" {
		usageControlEnabled = true
	}

	dataExportEnabled := false
	dataExport := pvc.Labels[MessageQueuePublishKey]
	if dataExport == "true" {
		dataExportEnabled = true
	}

	return usageControlEnabled, dataExportEnabled, nil
}
