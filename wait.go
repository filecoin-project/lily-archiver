package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func WaitUntil(ctx context.Context, condition func(context.Context) (bool, error), delay time.Duration, interval time.Duration) error {
	if delay > 0 {
		time.Sleep(delay)
	}
	done, err := condition(ctx)
	if err != nil {
		return err
	}
	if done {
		return nil
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

func Wait(ctx context.Context, condition func(context.Context) (bool, error)) error {
	done, err := condition(ctx)
	if err != nil {
		return err
	}
	if done {
		return nil
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	for {
		select {
		case <-c:
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
