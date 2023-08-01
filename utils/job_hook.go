package utils

import (
	"github.com/modern-go/gls"
	"github.com/sirupsen/logrus"
)

type Hook struct {
	Field  string
	levels []logrus.Level
}

func (hook *Hook) Levels() []logrus.Level {
	return hook.levels
}

func (hook *Hook) Fire(entry *logrus.Entry) error {
	syncName := gls.Get(hook.Field)
	if syncName != nil {
		entry.Data[hook.Field] = gls.Get(hook.Field)
	}
	return nil
}

func NewHook(levels ...logrus.Level) *Hook {
	hook := Hook{
		Field:  "job",
		levels: levels,
	}
	if len(hook.levels) == 0 {
		hook.levels = logrus.AllLevels
	}

	return &hook
}
