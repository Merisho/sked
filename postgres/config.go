package postgres

import "time"

const defaultLockDurationConfig = 30 * time.Second

type Config struct {
	LockDuration time.Duration
}

func (conf Config) prepare() Config {
	if conf.LockDuration <= 0 {
		conf.LockDuration = defaultLockDurationConfig
	}

	return conf
}
