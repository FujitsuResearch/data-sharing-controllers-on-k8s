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
	v1alpha1 "github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/apis/cr/core/v1alpha1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"
)

// RdbDataLister helps list RdbDatas.
// All objects returned here must be treated as read-only.
type RdbDataLister interface {
	// List lists all RdbDatas in the indexer.
	// Objects returned here must be treated as read-only.
	List(selector labels.Selector) (ret []*v1alpha1.RdbData, err error)
	// RdbDatas returns an object that can list and get RdbDatas.
	RdbDatas(namespace string) RdbDataNamespaceLister
	RdbDataListerExpansion
}

// rdbDataLister implements the RdbDataLister interface.
type rdbDataLister struct {
	indexer cache.Indexer
}

// NewRdbDataLister returns a new RdbDataLister.
func NewRdbDataLister(indexer cache.Indexer) RdbDataLister {
	return &rdbDataLister{indexer: indexer}
}

// List lists all RdbDatas in the indexer.
func (s *rdbDataLister) List(selector labels.Selector) (ret []*v1alpha1.RdbData, err error) {
	err = cache.ListAll(s.indexer, selector, func(m interface{}) {
		ret = append(ret, m.(*v1alpha1.RdbData))
	})
	return ret, err
}

// RdbDatas returns an object that can list and get RdbDatas.
func (s *rdbDataLister) RdbDatas(namespace string) RdbDataNamespaceLister {
	return rdbDataNamespaceLister{indexer: s.indexer, namespace: namespace}
}

// RdbDataNamespaceLister helps list and get RdbDatas.
// All objects returned here must be treated as read-only.
type RdbDataNamespaceLister interface {
	// List lists all RdbDatas in the indexer for a given namespace.
	// Objects returned here must be treated as read-only.
	List(selector labels.Selector) (ret []*v1alpha1.RdbData, err error)
	// Get retrieves the RdbData from the indexer for a given namespace and name.
	// Objects returned here must be treated as read-only.
	Get(name string) (*v1alpha1.RdbData, error)
	RdbDataNamespaceListerExpansion
}

// rdbDataNamespaceLister implements the RdbDataNamespaceLister
// interface.
type rdbDataNamespaceLister struct {
	indexer   cache.Indexer
	namespace string
}

// List lists all RdbDatas in the indexer for a given namespace.
func (s rdbDataNamespaceLister) List(selector labels.Selector) (ret []*v1alpha1.RdbData, err error) {
	err = cache.ListAllByNamespace(s.indexer, s.namespace, selector, func(m interface{}) {
		ret = append(ret, m.(*v1alpha1.RdbData))
	})
	return ret, err
}

// Get retrieves the RdbData from the indexer for a given namespace and name.
func (s rdbDataNamespaceLister) Get(name string) (*v1alpha1.RdbData, error) {
	obj, exists, err := s.indexer.GetByKey(s.namespace + "/" + name)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.NewNotFound(v1alpha1.Resource("rdbdata"), name)
	}
	return obj.(*v1alpha1.RdbData), nil
}
