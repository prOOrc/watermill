package saga

import (
	"fmt"

	"github.com/ThreeDotsLabs/watermill/components/cqrs"
)

type replyFactoryFunc func() interface{}

func newReplyFactory(
	replyFactories []func() cqrs.Reply,
) ReplyFactoryFunc {
	replyFactoriesMap := make(map[string]replyFactoryFunc, len(replyFactories))
	for _, f := range replyFactories {
		replyFactoriesMap[f().ReplyName()] = newFactory(f)
	}
	return func(replyName string) (interface{}, error) {
		factory, ok := replyFactoriesMap[replyName]
		if !ok {
			return nil, fmt.Errorf("factory for reply %s not found", replyName)
		}
		return factory(), nil
	}
}

func newFactory(f func() cqrs.Reply) replyFactoryFunc {
	return func() interface{} {return f()}
}
