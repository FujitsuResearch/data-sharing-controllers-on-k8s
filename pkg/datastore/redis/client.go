// Copyright (c) 2022 Fujitsu Limited

package redis

import (
	"encoding/json"
	"fmt"

	goredis "github.com/go-redis/redis/v8"
	"k8s.io/klog/v2"

	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/datastore"
	"github.com/FujitsuResearch/data-sharing-controllers-on-k8s/pkg/util"
)

type RedisClient struct {
	client *goredis.Client
}

func NewRedisValidationClient(
	address string) (datastore.ValidationDataStoreOperations, error) {
	client := goredis.NewClient(
		&goredis.Options{
			Addr: address,
		})

	ctx, cancel := util.GetTimeoutContext(util.DefaultDataStoreClientTimeout)
	defer cancel()

	err := client.Ping(ctx).Err()
	if err != nil {
		return nil, err
	}

	return &RedisClient{
		client: client,
	}, nil
}

func SetRedisClient(client *goredis.Client, redisClient *RedisClient) error {
	ctx, cancel := util.GetTimeoutContext(util.DefaultDataStoreClientTimeout)
	defer cancel()

	err := client.Ping(ctx).Err()
	if err != nil {
		return err
	}

	redisClient.client = client

	return nil
}

func (rc *RedisClient) Close() error {
	return rc.client.Close()
}

func (rc *RedisClient) GetDataLifetimeKeys() ([]string, error) {
	ctx, cancel := util.GetTimeoutContext(util.DefaultDataStoreClientTimeout)
	defer cancel()

	pattern := fmt.Sprintf("%s*", datastore.LifetimeCachePrefix)
	keys, err := rc.client.Keys(ctx, pattern).Result()
	if err != nil {
		return nil, err
	}

	strippedKeys := make([]string, 0, len(keys))
	prefixLen := len(datastore.LifetimeCachePrefix)
	for _, key := range keys {
		strippedKeys = append(strippedKeys, key[prefixLen:])
	}

	return strippedKeys, nil
}

func (rc *RedisClient) GetDataLifetime(
	key string) (*datastore.LifetimeCache, error) {
	lifetimeCacheKey := fmt.Sprintf("%s%s", datastore.LifetimeCachePrefix, key)

	ctx, cancel := util.GetTimeoutContext(util.DefaultDataStoreClientTimeout)
	defer cancel()

	value, err := rc.client.Get(ctx, lifetimeCacheKey).Result()
	if err != nil {
		if err == goredis.Nil {
			return nil, nil
		}

		return nil, err
	}

	var lifetimeCache datastore.LifetimeCache
	err = json.Unmarshal([]byte(value), &lifetimeCache)
	if err != nil {
		return nil, err
	}

	return &lifetimeCache, nil
}

func (rc *RedisClient) AddDataLifetime(
	key string, endTime string) (*datastore.LifetimeCache, error) {
	lifetimeCache, err := rc.GetDataLifetime(key)
	if err != nil {
		return nil, err
	}

	if lifetimeCache == nil {
		lifetimeCache = &datastore.LifetimeCache{
			EndTime: endTime,
		}
	} else {
		lifetimeCache.EndTime = endTime
		lifetimeCache.SuccessorPodTerminated = false
	}

	lifetimeStr, err := json.Marshal(lifetimeCache)
	if err != nil {
		return nil, err
	}

	lifetimeCacheKey := fmt.Sprintf("%s%s", datastore.LifetimeCachePrefix, key)

	ctx, cancel := util.GetTimeoutContext(util.DefaultDataStoreClientTimeout)
	defer cancel()

	err = rc.client.Set(ctx, lifetimeCacheKey, lifetimeStr, 0).Err()
	if err != nil {
		return nil, err
	}

	return lifetimeCache, nil
}

func (rc *RedisClient) DeleteDataLifetime(key string) error {
	lifetimeCache, err := rc.GetDataLifetime(key)
	if err != nil {
		return err
	}

	lifetimeCacheKey := fmt.Sprintf("%s%s", datastore.LifetimeCachePrefix, key)

	if len(lifetimeCache.SuccessorLifetimes) != 0 {
		lifetimeCache.SuccessorPodTerminated = true

		ctx, cancel := util.GetTimeoutContext(util.DefaultDataStoreClientTimeout)
		defer cancel()

		lifetimeStr, err := json.Marshal(lifetimeCache)
		if err != nil {
			return err
		}

		return rc.client.Set(ctx, lifetimeCacheKey, lifetimeStr, 0).Err()
	}

	ctx, cancel := util.GetTimeoutContext(util.DefaultDataStoreClientTimeout)
	defer cancel()

	return rc.client.Del(ctx, lifetimeCacheKey).Err()
}

func (rc *RedisClient) AddSuccessorDataLifetimeCustomResource(
	key string, successor string) error {
	lifetimeCache, err := rc.GetDataLifetime(key)
	if err != nil {
		return err
	}

	if lifetimeCache == nil {
		return fmt.Errorf("The lifetime cache for %q does NOT exist", key)
	}

	if lifetimeCache.SuccessorLifetimes == nil {
		lifetimeCache.SuccessorLifetimes = map[string]struct{}{}
	}

	lifetimeCache.SuccessorLifetimes[successor] = struct{}{}

	lifetimeStr, err := json.Marshal(lifetimeCache)
	if err != nil {
		return err
	}

	lifetimeCacheKey := fmt.Sprintf("%s%s", datastore.LifetimeCachePrefix, key)

	ctx, cancel := util.GetTimeoutContext(util.DefaultDataStoreClientTimeout)
	defer cancel()

	return rc.client.Set(ctx, lifetimeCacheKey, lifetimeStr, 0).Err()
}

func (rc *RedisClient) DeleteSuccessorDataLifetimeCustomResource(
	key string, successor string) error {
	lifetimeCache, err := rc.GetDataLifetime(key)
	if err != nil {
		return err
	} else if lifetimeCache == nil {
		klog.Warningf("The lifetime cache for %q does NOT exist", key)
		return nil
	}

	delete(lifetimeCache.SuccessorLifetimes, successor)

	lifetimeCacheKey := fmt.Sprintf("%s%s", datastore.LifetimeCachePrefix, key)

	if len(lifetimeCache.SuccessorLifetimes) == 0 &&
		lifetimeCache.SuccessorPodTerminated {
		ctx, cancel := util.GetTimeoutContext(
			util.DefaultDataStoreClientTimeout)
		defer cancel()

		return rc.client.Del(ctx, lifetimeCacheKey).Err()
	}

	lifetimeStr, err := json.Marshal(lifetimeCache)
	if err != nil {
		return err
	}

	ctx, cancel := util.GetTimeoutContext(util.DefaultDataStoreClientTimeout)
	defer cancel()

	return rc.client.Set(ctx, lifetimeCacheKey, lifetimeStr, 0).Err()
}
