package main

import (
	"context"
	"time"
)

func WaitUntil(ctx context.Context, condition func(context.Context) (bool, error), delay time.Duration, interval time.Duration) error {
	if delay > 0 {
		time.Sleep(delay)
	}

	tick := time.NewTicker(interval)
	defer tick.Stop()

	for {
		select {
		case <-tick.C:
			done, err := condition(ctx)
			if err != nil {
				return err
			}
			if done {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
