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

// RdbDatasGetter has a method to return a RdbDataInterface.
// A group's client should implement this interface.
type RdbDatasGetter interface {
	RdbDatas(namespace string) RdbDataInterface
}

// RdbDataInterface has methods to work with RdbData resources.
type RdbDataInterface interface {
	Create(ctx context.Context, rdbData *v1alpha1.RdbData, opts v1.CreateOptions) (*v1alpha1.RdbData, error)
	Update(ctx context.Context, rdbData *v1alpha1.RdbData, opts v1.UpdateOptions) (*v1alpha1.RdbData, error)
	UpdateStatus(ctx context.Context, rdbData *v1alpha1.RdbData, opts v1.UpdateOptions) (*v1alpha1.RdbData, error)
	Delete(ctx context.Context, name string, opts v1.DeleteOptions) error
	DeleteCollection(ctx context.Context, opts v1.DeleteOptions, listOpts v1.ListOptions) error
	Get(ctx context.Context, name string, opts v1.GetOptions) (*v1alpha1.RdbData, error)
	List(ctx context.Context, opts v1.ListOptions) (*v1alpha1.RdbDataList, error)
	Watch(ctx context.Context, opts v1.ListOptions) (watch.Interface, error)
	Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts v1.PatchOptions, subresources ...string) (result *v1alpha1.RdbData, err error)
	RdbDataExpansion
}

// rdbDatas implements RdbDataInterface
type rdbDatas struct {
	client rest.Interface
	ns     string
}

// newRdbDatas returns a RdbDatas
func newRdbDatas(c *CoreV1alpha1Client, namespace string) *rdbDatas {
	return &rdbDatas{
		client: c.RESTClient(),
		ns:     namespace,
	}
}

// Get takes name of the rdbData, and returns the corresponding rdbData object, and an error if there is any.
func (c *rdbDatas) Get(ctx context.Context, name string, options v1.GetOptions) (result *v1alpha1.RdbData, err error) {
	result = &v1alpha1.RdbData{}
	err = c.client.Get().
		Namespace(c.ns).
		Resource("rdbdatas").
		Name(name).
		VersionedParams(&options, scheme.ParameterCodec).
		Do(ctx).
		Into(result)
	return
}

// List takes label and field selectors, and returns the list of RdbDatas that match those selectors.
func (c *rdbDatas) List(ctx context.Context, opts v1.ListOptions) (result *v1alpha1.RdbDataList, err error) {
	var timeout time.Duration
	if opts.TimeoutSeconds != nil {
		timeout = time.Duration(*opts.TimeoutSeconds) * time.Second
	}
	result = &v1alpha1.RdbDataList{}
	err = c.client.Get().
		Namespace(c.ns).
		Resource("rdbdatas").
		VersionedParams(&opts, scheme.ParameterCodec).
		Timeout(timeout).
		Do(ctx).
		Into(result)
	return
}

// Watch returns a watch.Interface that watches the requested rdbDatas.
func (c *rdbDatas) Watch(ctx context.Context, opts v1.ListOptions) (watch.Interface, error) {
	var timeout time.Duration
	if opts.TimeoutSeconds != nil {
		timeout = time.Duration(*opts.TimeoutSeconds) * time.Second
	}
	opts.Watch = true
	return c.client.Get().
		Namespace(c.ns).
		Resource("rdbdatas").
		VersionedParams(&opts, scheme.ParameterCodec).
		Timeout(timeout).
		Watch(ctx)
}

// Create takes the representation of a rdbData and creates it.  Returns the server's representation of the rdbData, and an error, if there is any.
func (c *rdbDatas) Create(ctx context.Context, rdbData *v1alpha1.RdbData, opts v1.CreateOptions) (result *v1alpha1.RdbData, err error) {
	result = &v1alpha1.RdbData{}
	err = c.client.Post().
		Namespace(c.ns).
		Resource("rdbdatas").
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(rdbData).
		Do(ctx).
		Into(result)
	return
}

// Update takes the representation of a rdbData and updates it. Returns the server's representation of the rdbData, and an error, if there is any.
func (c *rdbDatas) Update(ctx context.Context, rdbData *v1alpha1.RdbData, opts v1.UpdateOptions) (result *v1alpha1.RdbData, err error) {
	result = &v1alpha1.RdbData{}
	err = c.client.Put().
		Namespace(c.ns).
		Resource("rdbdatas").
		Name(rdbData.Name).
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(rdbData).
		Do(ctx).
		Into(result)
	return
}

// UpdateStatus was generated because the type contains a Status member.
// Add a +genclient:noStatus comment above the type to avoid generating UpdateStatus().
func (c *rdbDatas) UpdateStatus(ctx context.Context, rdbData *v1alpha1.RdbData, opts v1.UpdateOptions) (result *v1alpha1.RdbData, err error) {
	result = &v1alpha1.RdbData{}
	err = c.client.Put().
		Namespace(c.ns).
		Resource("rdbdatas").
		Name(rdbData.Name).
		SubResource("status").
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(rdbData).
		Do(ctx).
		Into(result)
	return
}

// Delete takes name of the rdbData and deletes it. Returns an error if one occurs.
func (c *rdbDatas) Delete(ctx context.Context, name string, opts v1.DeleteOptions) error {
	return c.client.Delete().
		Namespace(c.ns).
		Resource("rdbdatas").
		Name(name).
		Body(&opts).
		Do(ctx).
		Error()
}

// DeleteCollection deletes a collection of objects.
func (c *rdbDatas) DeleteCollection(ctx context.Context, opts v1.DeleteOptions, listOpts v1.ListOptions) error {
	var timeout time.Duration
	if listOpts.TimeoutSeconds != nil {
		timeout = time.Duration(*listOpts.TimeoutSeconds) * time.Second
	}
	return c.client.Delete().
		Namespace(c.ns).
		Resource("rdbdatas").
		VersionedParams(&listOpts, scheme.ParameterCodec).
		Timeout(timeout).
		Body(&opts).
		Do(ctx).
		Error()
}

// Patch applies the patch and returns the patched rdbData.
func (c *rdbDatas) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts v1.PatchOptions, subresources ...string) (result *v1alpha1.RdbData, err error) {
	result = &v1alpha1.RdbData{}
	err = c.client.Patch(pt).
		Namespace(c.ns).
		Resource("rdbdatas").
		Name(name).
		SubResource(subresources...).
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(data).
		Do(ctx).
		Into(result)
	return
}