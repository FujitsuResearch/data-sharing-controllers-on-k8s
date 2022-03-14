// Copyright (c) 2022 Fujitsu Limited

package redis

import (
	"fmt"
	"testing"
	"time"

	goredis "github.com/go-redis/redis/v8"

	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/datastore"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
)

func newFakeRedisValidationClient(
	miniRedis *miniredis.Miniredis) datastore.ValidationDataStoreOperations {
	client := goredis.NewClient(
		&goredis.Options{
			Addr: miniRedis.Addr(),
		})

	return &RedisClient{
		client: client,
	}
}

func TestAddDataLifetime(t *testing.T) {
	miniRedis, err := miniredis.Run()
	assert.NoError(t, err)

	redisClient := newFakeRedisValidationClient(miniRedis)
	defer redisClient.Close()

	key := "key1"
	now := time.Now()
	endTime := now.Format(time.RFC3339)
	lifetimeCache, err := redisClient.AddDataLifetime(key, endTime)
	assert.NoError(t, err)
	expectedLifetimeCache := &datastore.LifetimeCache{
		EndTime: endTime,
	}
	assert.Equal(t, expectedLifetimeCache, lifetimeCache)
}

func TestUpdateDataLifetime(t *testing.T) {
	miniRedis, err := miniredis.Run()
	assert.NoError(t, err)

	redisClient := newFakeRedisValidationClient(miniRedis)
	defer redisClient.Close()

	key := "key1"
	now := time.Now()
	endTime1 := now.Format(time.RFC3339)
	lifetimeCache, err := redisClient.AddDataLifetime(key, endTime1)
	assert.NoError(t, err)
	expectedLifetimeCache := &datastore.LifetimeCache{
		EndTime: endTime1,
	}
	assert.Equal(t, expectedLifetimeCache, lifetimeCache)

	successor := "successor1"
	err = redisClient.AddSuccessorDataLifetimeCustomResource(key, successor)
	assert.NoError(t, err)

	lifetimeCache, err = redisClient.GetDataLifetime(key)
	assert.NoError(t, err)
	expectedLifetimeCache = &datastore.LifetimeCache{
		EndTime: endTime1,
		SuccessorLifetimes: map[string]struct{}{
			successor: struct{}{},
		},
	}
	assert.Equal(t, expectedLifetimeCache, lifetimeCache)

	err = redisClient.DeleteDataLifetime(key)
	assert.NoError(t, err)

	lifetimeCache, err = redisClient.GetDataLifetime(key)
	assert.NoError(t, err)
	expectedLifetimeCache = &datastore.LifetimeCache{
		EndTime: endTime1,
		SuccessorLifetimes: map[string]struct{}{
			successor: struct{}{},
		},
		SuccessorPodTerminated: true,
	}
	assert.Equal(t, expectedLifetimeCache, lifetimeCache)

	endTime2 := now.Add(2 * time.Minute).Format(time.RFC3339)
	lifetimeCache, err = redisClient.AddDataLifetime(key, endTime2)
	assert.NoError(t, err)
	expectedLifetimeCache = &datastore.LifetimeCache{
		EndTime: endTime2,
		SuccessorLifetimes: map[string]struct{}{
			successor: struct{}{},
		},
	}
	assert.Equal(t, expectedLifetimeCache, lifetimeCache)
}

func TestDeleteDataLifetime(t *testing.T) {
	miniRedis, err := miniredis.Run()
	assert.NoError(t, err)

	redisClient := newFakeRedisValidationClient(miniRedis)
	defer redisClient.Close()

	key := "key1"
	now := time.Now()
	endTime := now.Format(time.RFC3339)
	lifetimeCache, err := redisClient.AddDataLifetime(key, endTime)
	assert.NoError(t, err)
	expectedLifetimeCache := &datastore.LifetimeCache{
		EndTime: endTime,
	}
	assert.Equal(t, expectedLifetimeCache, lifetimeCache)

	err = redisClient.DeleteDataLifetime(key)
	assert.NoError(t, err)

	lifetimeCache, err = redisClient.GetDataLifetime(key)
	assert.NoError(t, err)
	assert.Nil(t, lifetimeCache)
}

func TestDeleteDataLifetimeForSuccessorsExist(t *testing.T) {
	miniRedis, err := miniredis.Run()
	assert.NoError(t, err)

	redisClient := newFakeRedisValidationClient(miniRedis)
	defer redisClient.Close()

	key := "key1"
	now := time.Now()
	endTime := now.Format(time.RFC3339)
	lifetimeCache, err := redisClient.AddDataLifetime(key, endTime)
	assert.NoError(t, err)
	expectedLifetimeCache := &datastore.LifetimeCache{
		EndTime: endTime,
	}
	assert.Equal(t, expectedLifetimeCache, lifetimeCache)

	successor := "successor1"
	err = redisClient.AddSuccessorDataLifetimeCustomResource(key, successor)
	assert.NoError(t, err)

	lifetimeCache, err = redisClient.GetDataLifetime(key)
	assert.NoError(t, err)
	expectedLifetimeCache = &datastore.LifetimeCache{
		EndTime: endTime,
		SuccessorLifetimes: map[string]struct{}{
			successor: struct{}{},
		},
	}
	assert.Equal(t, expectedLifetimeCache, lifetimeCache)

	err = redisClient.DeleteDataLifetime(key)
	assert.NoError(t, err)

	lifetimeCache, err = redisClient.GetDataLifetime(key)
	assert.NoError(t, err)
	expectedLifetimeCache = &datastore.LifetimeCache{
		EndTime: endTime,
		SuccessorLifetimes: map[string]struct{}{
			successor: struct{}{},
		},
		SuccessorPodTerminated: true,
	}
	assert.Equal(t, expectedLifetimeCache, lifetimeCache)
}

func TestAddSuccessorDataLifetimeCustomResource(t *testing.T) {
	miniRedis, err := miniredis.Run()
	assert.NoError(t, err)

	redisClient := newFakeRedisValidationClient(miniRedis)
	defer redisClient.Close()

	key := "key1"
	now := time.Now()
	endTime := now.Format(time.RFC3339)
	lifetimeCache, err := redisClient.AddDataLifetime(key, endTime)
	assert.NoError(t, err)
	expectedLifetimeCache := &datastore.LifetimeCache{
		EndTime: endTime,
	}
	assert.Equal(t, expectedLifetimeCache, lifetimeCache)

	successor := "successor1"
	err = redisClient.AddSuccessorDataLifetimeCustomResource(key, successor)
	assert.NoError(t, err)

	lifetimeCache, err = redisClient.GetDataLifetime(key)
	assert.NoError(t, err)
	expectedLifetimeCache = &datastore.LifetimeCache{
		EndTime: endTime,
		SuccessorLifetimes: map[string]struct{}{
			successor: struct{}{},
		},
	}
	assert.Equal(t, expectedLifetimeCache, lifetimeCache)
}

func TestAddSuccessorDataLifetimeCustomResourceForKeyNotFound(t *testing.T) {
	miniRedis, err := miniredis.Run()
	assert.NoError(t, err)

	redisClient := newFakeRedisValidationClient(miniRedis)
	defer redisClient.Close()

	key1 := "key1"
	now := time.Now()
	endTime := now.Format(time.RFC3339)
	lifetimeCache, err := redisClient.AddDataLifetime(key1, endTime)
	assert.NoError(t, err)
	expectedLifetimeCache := &datastore.LifetimeCache{
		EndTime: endTime,
	}
	assert.Equal(t, expectedLifetimeCache, lifetimeCache)

	key2 := "key2"
	successor := "successor1"
	err = redisClient.AddSuccessorDataLifetimeCustomResource(key2, successor)
	assert.Error(t, err)
	assert.EqualError(
		t, err, fmt.Sprintf("The lifetime cache for %q does NOT exist", key2))
}

func TestDeleteSuccessorDataLifetimeCustomResource(t *testing.T) {
	miniRedis, err := miniredis.Run()
	assert.NoError(t, err)

	redisClient := newFakeRedisValidationClient(miniRedis)
	defer redisClient.Close()

	key := "key1"
	now := time.Now()
	endTime := now.Format(time.RFC3339)
	lifetimeCache, err := redisClient.AddDataLifetime(key, endTime)
	assert.NoError(t, err)
	expectedLifetimeCache := &datastore.LifetimeCache{
		EndTime: endTime,
	}
	assert.Equal(t, expectedLifetimeCache, lifetimeCache)

	successor := "successor1"
	err = redisClient.AddSuccessorDataLifetimeCustomResource(key, successor)
	assert.NoError(t, err)

	lifetimeCache, err = redisClient.GetDataLifetime(key)
	assert.NoError(t, err)
	expectedLifetimeCache = &datastore.LifetimeCache{
		EndTime: endTime,
		SuccessorLifetimes: map[string]struct{}{
			successor: struct{}{},
		},
	}
	assert.Equal(t, expectedLifetimeCache, lifetimeCache)

	err = redisClient.DeleteSuccessorDataLifetimeCustomResource(key, successor)
	assert.NoError(t, err)

	lifetimeCache, err = redisClient.GetDataLifetime(key)
	assert.NoError(t, err)
	expectedLifetimeCache = &datastore.LifetimeCache{
		EndTime: endTime,
	}
	assert.Equal(t, expectedLifetimeCache, lifetimeCache)
}

func TestDeleteSuccessorDataLifetimeCustomResourceWithDataLifetimeDeletion(
	t *testing.T) {
	miniRedis, err := miniredis.Run()
	assert.NoError(t, err)

	redisClient := newFakeRedisValidationClient(miniRedis)
	defer redisClient.Close()

	key := "key1"
	now := time.Now()
	endTime := now.Format(time.RFC3339)
	lifetimeCache, err := redisClient.AddDataLifetime(key, endTime)
	assert.NoError(t, err)
	expectedLifetimeCache := &datastore.LifetimeCache{
		EndTime: endTime,
	}
	assert.Equal(t, expectedLifetimeCache, lifetimeCache)

	successor := "successor1"
	err = redisClient.AddSuccessorDataLifetimeCustomResource(key, successor)
	assert.NoError(t, err)

	lifetimeCache, err = redisClient.GetDataLifetime(key)
	assert.NoError(t, err)
	expectedLifetimeCache = &datastore.LifetimeCache{
		EndTime: endTime,
		SuccessorLifetimes: map[string]struct{}{
			successor: struct{}{},
		},
	}
	assert.Equal(t, expectedLifetimeCache, lifetimeCache)

	err = redisClient.DeleteDataLifetime(key)
	assert.NoError(t, err)

	lifetimeCache, err = redisClient.GetDataLifetime(key)
	assert.NoError(t, err)
	expectedLifetimeCache = &datastore.LifetimeCache{
		EndTime: endTime,
		SuccessorLifetimes: map[string]struct{}{
			successor: struct{}{},
		},
		SuccessorPodTerminated: true,
	}
	assert.Equal(t, expectedLifetimeCache, lifetimeCache)

	err = redisClient.DeleteSuccessorDataLifetimeCustomResource(key, successor)
	assert.NoError(t, err)

	lifetimeCache, err = redisClient.GetDataLifetime(key)
	assert.NoError(t, err)
	assert.Nil(t, lifetimeCache)
}

func TestDeleteSuccessorDataLifetimeCustomResourceForKeyNotFound(
	t *testing.T) {
	miniRedis, err := miniredis.Run()
	assert.NoError(t, err)

	redisClient := newFakeRedisValidationClient(miniRedis)
	defer redisClient.Close()

	key1 := "key1"
	now := time.Now()
	endTime := now.Format(time.RFC3339)
	lifetimeCache, err := redisClient.AddDataLifetime(key1, endTime)
	assert.NoError(t, err)
	expectedLifetimeCache := &datastore.LifetimeCache{
		EndTime: endTime,
	}
	assert.Equal(t, expectedLifetimeCache, lifetimeCache)

	successor := "successor1"
	err = redisClient.AddSuccessorDataLifetimeCustomResource(key1, successor)
	assert.NoError(t, err)

	lifetimeCache, err = redisClient.GetDataLifetime(key1)
	assert.NoError(t, err)
	expectedLifetimeCache = &datastore.LifetimeCache{
		EndTime: endTime,
		SuccessorLifetimes: map[string]struct{}{
			successor: struct{}{},
		},
	}
	assert.Equal(t, expectedLifetimeCache, lifetimeCache)

	key2 := "key2"
	err = redisClient.DeleteSuccessorDataLifetimeCustomResource(
		key2, successor)
	assert.NoError(t, err)
}
