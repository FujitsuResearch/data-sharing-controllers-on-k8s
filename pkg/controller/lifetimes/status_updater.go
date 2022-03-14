// Copyright (c) 2022 Fujitsu Limited

package lifetimes

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	lifetimesapi "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/cr/lifetimes/v1alpha1"
	lifetimesclientset "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/client/cr/lifetimes/clientset/versioned"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/util"
)

type updateStatus struct {
	dataName   string
	timestamps lifetimesapi.ConsumerStatus
}

type statusUpdater struct {
	dataLifetimesClient  lifetimesclientset.Interface
	lifetime             *lifetimesapi.DataLifetime
	namespace            string
	statusCh             <-chan updateStatus
	stopCh               <-chan struct{}
	lifetimeSpecUpdateCh <-chan lifetimePolicyUpdate
}

func newStatusUpdater(
	dataLifetimesClient lifetimesclientset.Interface,
	lifetime *lifetimesapi.DataLifetime, namespace string,
	statusCh <-chan updateStatus, stopCh <-chan struct{},
	lifetimeSpecUpdateCh <-chan lifetimePolicyUpdate) *statusUpdater {
	lifetime.Status = lifetimesapi.LifetimeStatus{
		Consumer: map[string]lifetimesapi.ConsumerStatus{},
	}

	return &statusUpdater{
		dataLifetimesClient:  dataLifetimesClient,
		lifetime:             lifetime,
		namespace:            namespace,
		statusCh:             statusCh,
		stopCh:               stopCh,
		lifetimeSpecUpdateCh: lifetimeSpecUpdateCh,
	}
}

func (su *statusUpdater) updateLifetimeStatus(status updateStatus) {
	su.lifetime.Status.Consumer[status.dataName] = status.timestamps

	ctx, cancel := util.GetTimeoutContext(util.DefaultKubeClientTimeout)
	defer cancel()

	newLifetime, err := su.dataLifetimesClient.LifetimesV1alpha1().
		DataLifetimes(su.namespace).Update(
		ctx, su.lifetime, metav1.UpdateOptions{})
	if err != nil {
		klog.Errorf(
			"Failed to update the lifetime status '%s/%s': %v",
			su.namespace, su.lifetime.Name, err)
	}

	su.lifetime = newLifetime
}

func (su *statusUpdater) start() {
	for {
		select {
		case status := <-su.statusCh:
			su.updateLifetimeStatus(status)
		case lifetimeSpec := <-su.lifetimeSpecUpdateCh:
			su.lifetime.Spec = lifetimeSpec.spec
		case <-su.stopCh:
			return
		}
	}
}
