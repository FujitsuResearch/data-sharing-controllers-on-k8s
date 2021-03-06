/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Code generated by client-gen. DO NOT EDIT.

package v1alpha1

import (
	"context"
	"time"

	v1alpha1 "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/cr/core/v1alpha1"
	scheme "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/client/cr/core/clientset/versioned/scheme"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	types "k8s.io/apimachinery/pkg/types"
	watch "k8s.io/apimachinery/pkg/watch"
	rest "k8s.io/client-go/rest"
)

// MessageQueuesGetter has a method to return a MessageQueueInterface.
// A group's client should implement this interface.
type MessageQueuesGetter interface {
	MessageQueues(namespace string) MessageQueueInterface
}

// MessageQueueInterface has methods to work with MessageQueue resources.
type MessageQueueInterface interface {
	Create(ctx context.Context, messageQueue *v1alpha1.MessageQueue, opts v1.CreateOptions) (*v1alpha1.MessageQueue, error)
	Update(ctx context.Context, messageQueue *v1alpha1.MessageQueue, opts v1.UpdateOptions) (*v1alpha1.MessageQueue, error)
	UpdateStatus(ctx context.Context, messageQueue *v1alpha1.MessageQueue, opts v1.UpdateOptions) (*v1alpha1.MessageQueue, error)
	Delete(ctx context.Context, name string, opts v1.DeleteOptions) error
	DeleteCollection(ctx context.Context, opts v1.DeleteOptions, listOpts v1.ListOptions) error
	Get(ctx context.Context, name string, opts v1.GetOptions) (*v1alpha1.MessageQueue, error)
	List(ctx context.Context, opts v1.ListOptions) (*v1alpha1.MessageQueueList, error)
	Watch(ctx context.Context, opts v1.ListOptions) (watch.Interface, error)
	Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts v1.PatchOptions, subresources ...string) (result *v1alpha1.MessageQueue, err error)
	MessageQueueExpansion
}

// messageQueues implements MessageQueueInterface
type messageQueues struct {
	client rest.Interface
	ns     string
}

// newMessageQueues returns a MessageQueues
func newMessageQueues(c *CoreV1alpha1Client, namespace string) *messageQueues {
	return &messageQueues{
		client: c.RESTClient(),
		ns:     namespace,
	}
}

// Get takes name of the messageQueue, and returns the corresponding messageQueue object, and an error if there is any.
func (c *messageQueues) Get(ctx context.Context, name string, options v1.GetOptions) (result *v1alpha1.MessageQueue, err error) {
	result = &v1alpha1.MessageQueue{}
	err = c.client.Get().
		Namespace(c.ns).
		Resource("messagequeues").
		Name(name).
		VersionedParams(&options, scheme.ParameterCodec).
		Do(ctx).
		Into(result)
	return
}

// List takes label and field selectors, and returns the list of MessageQueues that match those selectors.
func (c *messageQueues) List(ctx context.Context, opts v1.ListOptions) (result *v1alpha1.MessageQueueList, err error) {
	var timeout time.Duration
	if opts.TimeoutSeconds != nil {
		timeout = time.Duration(*opts.TimeoutSeconds) * time.Second
	}
	result = &v1alpha1.MessageQueueList{}
	err = c.client.Get().
		Namespace(c.ns).
		Resource("messagequeues").
		VersionedParams(&opts, scheme.ParameterCodec).
		Timeout(timeout).
		Do(ctx).
		Into(result)
	return
}

// Watch returns a watch.Interface that watches the requested messageQueues.
func (c *messageQueues) Watch(ctx context.Context, opts v1.ListOptions) (watch.Interface, error) {
	var timeout time.Duration
	if opts.TimeoutSeconds != nil {
		timeout = time.Duration(*opts.TimeoutSeconds) * time.Second
	}
	opts.Watch = true
	return c.client.Get().
		Namespace(c.ns).
		Resource("messagequeues").
		VersionedParams(&opts, scheme.ParameterCodec).
		Timeout(timeout).
		Watch(ctx)
}

// Create takes the representation of a messageQueue and creates it.  Returns the server's representation of the messageQueue, and an error, if there is any.
func (c *messageQueues) Create(ctx context.Context, messageQueue *v1alpha1.MessageQueue, opts v1.CreateOptions) (result *v1alpha1.MessageQueue, err error) {
	result = &v1alpha1.MessageQueue{}
	err = c.client.Post().
		Namespace(c.ns).
		Resource("messagequeues").
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(messageQueue).
		Do(ctx).
		Into(result)
	return
}

// Update takes the representation of a messageQueue and updates it. Returns the server's representation of the messageQueue, and an error, if there is any.
func (c *messageQueues) Update(ctx context.Context, messageQueue *v1alpha1.MessageQueue, opts v1.UpdateOptions) (result *v1alpha1.MessageQueue, err error) {
	result = &v1alpha1.MessageQueue{}
	err = c.client.Put().
		Namespace(c.ns).
		Resource("messagequeues").
		Name(messageQueue.Name).
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(messageQueue).
		Do(ctx).
		Into(result)
	return
}

// UpdateStatus was generated because the type contains a Status member.
// Add a +genclient:noStatus comment above the type to avoid generating UpdateStatus().
func (c *messageQueues) UpdateStatus(ctx context.Context, messageQueue *v1alpha1.MessageQueue, opts v1.UpdateOptions) (result *v1alpha1.MessageQueue, err error) {
	result = &v1alpha1.MessageQueue{}
	err = c.client.Put().
		Namespace(c.ns).
		Resource("messagequeues").
		Name(messageQueue.Name).
		SubResource("status").
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(messageQueue).
		Do(ctx).
		Into(result)
	return
}

// Delete takes name of the messageQueue and deletes it. Returns an error if one occurs.
func (c *messageQueues) Delete(ctx context.Context, name string, opts v1.DeleteOptions) error {
	return c.client.Delete().
		Namespace(c.ns).
		Resource("messagequeues").
		Name(name).
		Body(&opts).
		Do(ctx).
		Error()
}

// DeleteCollection deletes a collection of objects.
func (c *messageQueues) DeleteCollection(ctx context.Context, opts v1.DeleteOptions, listOpts v1.ListOptions) error {
	var timeout time.Duration
	if listOpts.TimeoutSeconds != nil {
		timeout = time.Duration(*listOpts.TimeoutSeconds) * time.Second
	}
	return c.client.Delete().
		Namespace(c.ns).
		Resource("messagequeues").
		VersionedParams(&listOpts, scheme.ParameterCodec).
		Timeout(timeout).
		Body(&opts).
		Do(ctx).
		Error()
}

// Patch applies the patch and returns the patched messageQueue.
func (c *messageQueues) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts v1.PatchOptions, subresources ...string) (result *v1alpha1.MessageQueue, err error) {
	result = &v1alpha1.MessageQueue{}
	err = c.client.Patch(pt).
		Namespace(c.ns).
		Resource("messagequeues").
		Name(name).
		SubResource(subresources...).
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(data).
		Do(ctx).
		Into(result)
	return
}
