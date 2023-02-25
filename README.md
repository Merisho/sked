**Note: in development**

# What is it?
`sked` - an easy-to-use message scheduler for distributed systems. It is built to get the needed scheduling into your project with minimal costs.
Start with as minimal setup as putting the scheduling directly into your application code, or run a separate stand-alone scheduling service.

`sked` can be used to schedule any kind of messages for any kind of purposes - from simple reminders to scheduled task execution.

`sked` consists of two parts: the backend and the scheduler itself.
The backend provides persistence (if you need it), data management, and, most importantly, the locking mechanism for distributing the scheduled messages over the pool of workers.
The scheduler builds on top of the backend to schedule the messages and dispatch them as close to the scheduled time as possible.

At least once delivery is guaranteed.

# How to use it?

```
type MyReminder struct {
    User string
    Action string
}

func main() {
    db, err := sql.Open("pgx", "postgres://user:password@localhost:5432/reminders")
    if err != nil {
        panic(err)
    }
	
    ctx, cancel := context.WithCancel(context.Background())
	
    tableName := "user_reminders"
    pg := postgres.NewStore(db, tableName, postgres.Config{})
	
    err := pg.Bootstrap(ctx) // creates the necessary schemas in the DB
    if err != nil {
        panic(err)
    }
	
    errs := make(chan error, 1024)
    reminders := sked.NewSked[MyReminder](ctx, pg, sked.Option{Errors: errs})
	
    go func() {
        for err := range errs {
            log.Printf("Reminders scheduler error: %s\n", err)
        }
	    
        log.Println("Reminders scheduler stopped")
    }()

    aliceReminder := MyReminder{User: "Alice", Action: "Promote Bob to manager"}
    wtfValue, err := reminders.Schedule(time.Now().Add(5 * time.Second), aliceReminder)
    
    bobReminder := MyReminder{User: "Bob", Action: "Accept the promotion"}
    wtfValue, err = reminders.Schedule(time.Now().Add(10 * time.Second), bobReminder)
    
    for msg := range reminders.Messages() {
        fmt.Println("Reminding %s to '%s', reminder was scheduled at %s", msg.Payload.User, msg.Payload.Action, msg.ScheduledAt)
    }
}
```
