# cbk

A generic implementation of the observer pattern for async Rust.

## Usage

Create an observer that accepts some input:

```rust
use cbk::Observable;

#[derive(Debug)]
struct ExampleInputType;

async fn example_handler(something: &ExampleInputType) {
    println!("This is just an example! {something:?}")
}

async fn example() {
    let mut observable: Observable<ExampleInputType> = Observable::new();

    // Register some handlers
    observable.register(example_handler)
    observable.register(async |something: &ExampleType| {
        println!("Async closures work too: {something:?}")
    });
    observable.register(|something: &ExampleType| async {
        println!("So do sync closures that return a future: {something:?}")
    });

    // ... assume `observable` has been moved around arbitrarily in code

    // This will call all of the registered handlers
    observable.call(&ExampleType);
}
```

That's it! Context may also be passed in if some data accesible at registration time need be there at call time:

```rust
#[derive(Debug)]
struct ExampleType2(u8);

async fn example_handler_2(something: &ExampleType, context: &ExampleType2) {
    println!("This is just an example! {something:?}, {context:?}")
}

async fn example() {
    let mut observable = Observable::<ExampleType>::new();

    observable.register_with_context(example_handler_2, ExampleType2(22));

    observable.call(&ExampleType);
}
```
