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

	v1alpha1 "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/cr/core/v1alpha1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	labels "k8s.io/apimachinery/pkg/labels"
	schema "k8s.io/apimachinery/pkg/runtime/schema"
	types "k8s.io/apimachinery/pkg/types"
	watch "k8s.io/apimachinery/pkg/watch"
	testing "k8s.io/client-go/testing"
)

// FakeRdbDatas implements RdbDataInterface
type FakeRdbDatas struct {
	Fake *FakeCoreV1alpha1
	ns   string
}

var rdbdatasResource = schema.GroupVersionResource{Group: "core.dsc", Version: "v1alpha1", Resource: "rdbdatas"}

var rdbdatasKind = schema.GroupVersionKind{Group: "core.dsc", Version: "v1alpha1", Kind: "RdbData"}

// Get takes name of the rdbData, and returns the corresponding rdbData object, and an error if there is any.
func (c *FakeRdbDatas) Get(ctx context.Context, name string, options v1.GetOptions) (result *v1alpha1.RdbData, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewGetAction(rdbdatasResource, c.ns, name), &v1alpha1.RdbData{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1alpha1.RdbData), err
}

// List takes label and field selectors, and returns the list of RdbDatas that match those selectors.
func (c *FakeRdbDatas) List(ctx context.Context, opts v1.ListOptions) (result *v1alpha1.RdbDataList, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewListAction(rdbdatasResource, rdbdatasKind, c.ns, opts), &v1alpha1.RdbDataList{})

	if obj == nil {
		return nil, err
	}

	label, _, _ := testing.ExtractFromListOptions(opts)
	if label == nil {
		label = labels.Everything()
	}
	list := &v1alpha1.RdbDataList{ListMeta: obj.(*v1alpha1.RdbDataList).ListMeta}
	for _, item := range obj.(*v1alpha1.RdbDataList).Items {
		if label.Matches(labels.Set(item.Labels)) {
			list.Items = append(list.Items, item)
		}
	}
	return list, err
}

// Watch returns a watch.Interface that watches the requested rdbDatas.
func (c *FakeRdbDatas) Watch(ctx context.Context, opts v1.ListOptions) (watch.Interface, error) {
	return c.Fake.
		InvokesWatch(testing.NewWatchAction(rdbdatasResource, c.ns, opts))

}

// Create takes the representation of a rdbData and creates it.  Returns the server's representation of the rdbData, and an error, if there is any.
func (c *FakeRdbDatas) Create(ctx context.Context, rdbData *v1alpha1.RdbData, opts v1.CreateOptions) (result *v1alpha1.RdbData, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewCreateAction(rdbdatasResource, c.ns, rdbData), &v1alpha1.RdbData{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1alpha1.RdbData), err
}

// Update takes the representation of a rdbData and updates it. Returns the server's representation of the rdbData, and an error, if there is any.
func (c *FakeRdbDatas) Update(ctx context.Context, rdbData *v1alpha1.RdbData, opts v1.UpdateOptions) (result *v1alpha1.RdbData, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewUpdateAction(rdbdatasResource, c.ns, rdbData), &v1alpha1.RdbData{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1alpha1.RdbData), err
}

// UpdateStatus was generated because the type contains a Status member.
// Add a +genclient:noStatus comment above the type to avoid generating UpdateStatus().
func (c *FakeRdbDatas) UpdateStatus(ctx context.Context, rdbData *v1alpha1.RdbData, opts v1.UpdateOptions) (*v1alpha1.RdbData, error) {
	obj, err := c.Fake.
		Invokes(testing.NewUpdateSubresourceAction(rdbdatasResource, "status", c.ns, rdbData), &v1alpha1.RdbData{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1alpha1.RdbData), err
}

// Delete takes name of the rdbData and deletes it. Returns an error if one occurs.
func (c *FakeRdbDatas) Delete(ctx context.Context, name string, opts v1.DeleteOptions) error {
	_, err := c.Fake.
		Invokes(testing.NewDeleteActionWithOptions(rdbdatasResource, c.ns, name, opts), &v1alpha1.RdbData{})

	return err
}

// DeleteCollection deletes a collection of objects.
func (c *FakeRdbDatas) DeleteCollection(ctx context.Context, opts v1.DeleteOptions, listOpts v1.ListOptions) error {
	action := testing.NewDeleteCollectionAction(rdbdatasResource, c.ns, listOpts)

	_, err := c.Fake.Invokes(action, &v1alpha1.RdbDataList{})
	return err
}

// Patch applies the patch and returns the patched rdbData.
func (c *FakeRdbDatas) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts v1.PatchOptions, subresources ...string) (result *v1alpha1.RdbData, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewPatchSubresourceAction(rdbdatasResource, c.ns, name, pt, data, subresources...), &v1alpha1.RdbData{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1alpha1.RdbData), err
}
