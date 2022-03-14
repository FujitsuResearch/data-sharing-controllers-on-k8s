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

// Code generated by lister-gen. DO NOT EDIT.

package v1alpha1

import (
	v1alpha1 "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/cr/lifetimes/v1alpha1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"
)

// DataLifetimeLister helps list DataLifetimes.
// All objects returned here must be treated as read-only.
type DataLifetimeLister interface {
	// List lists all DataLifetimes in the indexer.
	// Objects returned here must be treated as read-only.
	List(selector labels.Selector) (ret []*v1alpha1.DataLifetime, err error)
	// DataLifetimes returns an object that can list and get DataLifetimes.
	DataLifetimes(namespace string) DataLifetimeNamespaceLister
	DataLifetimeListerExpansion
}

// dataLifetimeLister implements the DataLifetimeLister interface.
type dataLifetimeLister struct {
	indexer cache.Indexer
}

// NewDataLifetimeLister returns a new DataLifetimeLister.
func NewDataLifetimeLister(indexer cache.Indexer) DataLifetimeLister {
	return &dataLifetimeLister{indexer: indexer}
}

// List lists all DataLifetimes in the indexer.
func (s *dataLifetimeLister) List(selector labels.Selector) (ret []*v1alpha1.DataLifetime, err error) {
	err = cache.ListAll(s.indexer, selector, func(m interface{}) {
		ret = append(ret, m.(*v1alpha1.DataLifetime))
	})
	return ret, err
}

// DataLifetimes returns an object that can list and get DataLifetimes.
func (s *dataLifetimeLister) DataLifetimes(namespace string) DataLifetimeNamespaceLister {
	return dataLifetimeNamespaceLister{indexer: s.indexer, namespace: namespace}
}

// DataLifetimeNamespaceLister helps list and get DataLifetimes.
// All objects returned here must be treated as read-only.
type DataLifetimeNamespaceLister interface {
	// List lists all DataLifetimes in the indexer for a given namespace.
	// Objects returned here must be treated as read-only.
	List(selector labels.Selector) (ret []*v1alpha1.DataLifetime, err error)
	// Get retrieves the DataLifetime from the indexer for a given namespace and name.
	// Objects returned here must be treated as read-only.
	Get(name string) (*v1alpha1.DataLifetime, error)
	DataLifetimeNamespaceListerExpansion
}

// dataLifetimeNamespaceLister implements the DataLifetimeNamespaceLister
// interface.
type dataLifetimeNamespaceLister struct {
	indexer   cache.Indexer
	namespace string
}

// List lists all DataLifetimes in the indexer for a given namespace.
func (s dataLifetimeNamespaceLister) List(selector labels.Selector) (ret []*v1alpha1.DataLifetime, err error) {
	err = cache.ListAllByNamespace(s.indexer, s.namespace, selector, func(m interface{}) {
		ret = append(ret, m.(*v1alpha1.DataLifetime))
	})
	return ret, err
}

// Get retrieves the DataLifetime from the indexer for a given namespace and name.
func (s dataLifetimeNamespaceLister) Get(name string) (*v1alpha1.DataLifetime, error) {
	obj, exists, err := s.indexer.GetByKey(s.namespace + "/" + name)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.NewNotFound(v1alpha1.Resource("datalifetime"), name)
	}
	return obj.(*v1alpha1.DataLifetime), nil
}