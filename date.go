package main

import (
	"fmt"
	"time"
)

// Date is a calendar day in the UTC timezone
type Date struct {
	Year, Month, Day int
}

func (d *Date) String() string {
	return fmt.Sprintf("%04d-%02d-%02d", d.Year, d.Month, d.Day)
}

func (d Date) Time() time.Time {
	if d.IsZero() {
		return time.Time{}
	}
	return time.Date(d.Year, time.Month(d.Month), d.Day, 0, 0, 0, 0, time.UTC)
}

func (d Date) Next() Date {
	return DateFromTime(d.Time().AddDate(0, 0, 1))
}

func (d Date) Previous() Date {
	return DateFromTime(d.Time().AddDate(0, 0, -1))
}

func (d Date) After(d2 Date) bool {
	return d.Time().After(d2.Time())
}

func (d Date) IsZero() bool {
	return d == Date{}
}

func DateFromTs(ts int64) Date {
	dt := time.Unix(ts, 0).UTC()
	return DateFromTime(dt)
}

func DateFromTime(dt time.Time) Date {
	dtu := dt.UTC()
	return Date{
		Year:  dtu.Year(),
		Month: int(dtu.Month()),
		Day:   dtu.Day(),
	}
}

func DateFromString(s string) (Date, error) {
	dt, err := time.ParseInLocation("2006-01-02", s, time.UTC)
	if err != nil {
		return Date{}, err
	}
	return Date{
		Year:  dt.Year(),
		Month: int(dt.Month()),
		Day:   dt.Day(),
	}, nil
}

func Today() Date {
	return DateFromTime(time.Now())
}
