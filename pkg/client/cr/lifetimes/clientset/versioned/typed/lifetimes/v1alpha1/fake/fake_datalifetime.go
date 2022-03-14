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

package fake

import (
	"context"

	v1alpha1 "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/cr/lifetimes/v1alpha1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	labels "k8s.io/apimachinery/pkg/labels"
	schema "k8s.io/apimachinery/pkg/runtime/schema"
	types "k8s.io/apimachinery/pkg/types"
	watch "k8s.io/apimachinery/pkg/watch"
	testing "k8s.io/client-go/testing"
)

// FakeDataLifetimes implements DataLifetimeInterface
type FakeDataLifetimes struct {
	Fake *FakeLifetimesV1alpha1
	ns   string
}

var datalifetimesResource = schema.GroupVersionResource{Group: "lifetimes.dsc", Version: "v1alpha1", Resource: "datalifetimes"}

var datalifetimesKind = schema.GroupVersionKind{Group: "lifetimes.dsc", Version: "v1alpha1", Kind: "DataLifetime"}

// Get takes name of the dataLifetime, and returns the corresponding dataLifetime object, and an error if there is any.
func (c *FakeDataLifetimes) Get(ctx context.Context, name string, options v1.GetOptions) (result *v1alpha1.DataLifetime, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewGetAction(datalifetimesResource, c.ns, name), &v1alpha1.DataLifetime{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1alpha1.DataLifetime), err
}

// List takes label and field selectors, and returns the list of DataLifetimes that match those selectors.
func (c *FakeDataLifetimes) List(ctx context.Context, opts v1.ListOptions) (result *v1alpha1.DataLifetimeList, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewListAction(datalifetimesResource, datalifetimesKind, c.ns, opts), &v1alpha1.DataLifetimeList{})

	if obj == nil {
		return nil, err
	}

	label, _, _ := testing.ExtractFromListOptions(opts)
	if label == nil {
		label = labels.Everything()
	}
	list := &v1alpha1.DataLifetimeList{ListMeta: obj.(*v1alpha1.DataLifetimeList).ListMeta}
	for _, item := range obj.(*v1alpha1.DataLifetimeList).Items {
		if label.Matches(labels.Set(item.Labels)) {
			list.Items = append(list.Items, item)
		}
	}
	return list, err
}

// Watch returns a watch.Interface that watches the requested dataLifetimes.
func (c *FakeDataLifetimes) Watch(ctx context.Context, opts v1.ListOptions) (watch.Interface, error) {
	return c.Fake.
		InvokesWatch(testing.NewWatchAction(datalifetimesResource, c.ns, opts))

}

// Create takes the representation of a dataLifetime and creates it.  Returns the server's representation of the dataLifetime, and an error, if there is any.
func (c *FakeDataLifetimes) Create(ctx context.Context, dataLifetime *v1alpha1.DataLifetime, opts v1.CreateOptions) (result *v1alpha1.DataLifetime, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewCreateAction(datalifetimesResource, c.ns, dataLifetime), &v1alpha1.DataLifetime{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1alpha1.DataLifetime), err
}

// Update takes the representation of a dataLifetime and updates it. Returns the server's representation of the dataLifetime, and an error, if there is any.
func (c *FakeDataLifetimes) Update(ctx context.Context, dataLifetime *v1alpha1.DataLifetime, opts v1.UpdateOptions) (result *v1alpha1.DataLifetime, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewUpdateAction(datalifetimesResource, c.ns, dataLifetime), &v1alpha1.DataLifetime{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1alpha1.DataLifetime), err
}

// UpdateStatus was generated because the type contains a Status member.
// Add a +genclient:noStatus comment above the type to avoid generating UpdateStatus().
func (c *FakeDataLifetimes) UpdateStatus(ctx context.Context, dataLifetime *v1alpha1.DataLifetime, opts v1.UpdateOptions) (*v1alpha1.DataLifetime, error) {
	obj, err := c.Fake.
		Invokes(testing.NewUpdateSubresourceAction(datalifetimesResource, "status", c.ns, dataLifetime), &v1alpha1.DataLifetime{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1alpha1.DataLifetime), err
}

// Delete takes name of the dataLifetime and deletes it. Returns an error if one occurs.
func (c *FakeDataLifetimes) Delete(ctx context.Context, name string, opts v1.DeleteOptions) error {
	_, err := c.Fake.
		Invokes(testing.NewDeleteActionWithOptions(datalifetimesResource, c.ns, name, opts), &v1alpha1.DataLifetime{})

	return err
}

// DeleteCollection deletes a collection of objects.
func (c *FakeDataLifetimes) DeleteCollection(ctx context.Context, opts v1.DeleteOptions, listOpts v1.ListOptions) error {
	action := testing.NewDeleteCollectionAction(datalifetimesResource, c.ns, listOpts)

	_, err := c.Fake.Invokes(action, &v1alpha1.DataLifetimeList{})
	return err
}

// Patch applies the patch and returns the patched dataLifetime.
func (c *FakeDataLifetimes) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts v1.PatchOptions, subresources ...string) (result *v1alpha1.DataLifetime, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewPatchSubresourceAction(datalifetimesResource, c.ns, name, pt, data, subresources...), &v1alpha1.DataLifetime{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1alpha1.DataLifetime), err
}
