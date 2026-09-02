//! A tiny async observer/callback registry.
//!
//! [`Observable<T>`] stores a list of async handlers, each invoked with a shared
//! reference `&T` whenever [`Observable::call`] runs.
//!
//! ```
//! # use cbk::Observable;
//! # #[derive(Debug)] struct Event;
//! let mut observable = Observable::<Event>::new();
//!
//! // A free `async fn`, an `async` closure, and a plain closure returning a
//! // future are all accepted:
//! observable.register(async |e: &Event| println!("async closure: {e:?}"));
//! observable.register(|e: &Event| async move { println!("returns a future: {e:?}") });
//!
//! observable.call(&Event);
//! ```

use futures::future::BoxFuture;
use std::future::Future;

/// The type-erased handler stored inside the [`Observable`].
///
/// `'h` is the lifetime of the borrows an [`Observable`] hands out. The returned
/// future borrows `self` for the (shorter) call lifetime `'s`, which is what
/// allows a handler to lend the caller a reference into its own owned state
/// (see [`register_with_context`](Observable::register_with_context)) for the
/// duration of a single call.
trait Handler<'h, T> {
    fn call<'s>(&'s self, arg: &'h T) -> BoxFuture<'s, ()>
    where
        'h: 's;
}

/// Wrapper for a plain single-argument async callback.
struct Plain<F>(F);

impl<'h, T: 'h, F, Fut> Handler<'h, T> for Plain<F>
where
    F: Fn(&'h T) -> Fut,
    Fut: Future<Output = ()> + Send + 'h,
{
    fn call<'s>(&'s self, arg: &'h T) -> BoxFuture<'s, ()>
    where
        'h: 's,
    {
        Box::pin((self.0)(arg))
    }
}

/// Helper trait for the two-argument (context) case.
///
/// It exists to give the two-argument callback's returned future a nameable
/// associated type (`Fut`) that we can bound `Send`, while letting the context
/// reference use a fresh, per-call lifetime `'s` independent of the argument
/// lifetime `'h`. An `async fn(&T, &C)` naturally has two independent argument
/// lifetimes, so it satisfies this for any `'s`.
///
/// This is an implementation detail. It only appears in the bound of
/// [`Observable::register_with_context`]. It is `pub` solely to satisfy the
/// `private_bounds` lint and is hidden from the public API.
#[doc(hidden)]
pub trait ContextCallback<'h, 's, T: 'h, C: 's> {
    type Fut: Future<Output = ()> + Send + 's;
    fn call(&self, arg: &'h T, context: &'s C) -> Self::Fut;
}

impl<'h, 's, T: 'h, C: 's, F, Fut> ContextCallback<'h, 's, T, C> for F
where
    F: Fn(&'h T, &'s C) -> Fut,
    Fut: Future<Output = ()> + Send + 's,
{
    type Fut = Fut;
    fn call(&self, arg: &'h T, context: &'s C) -> Fut {
        self(arg, context)
    }
}

/// Wrapper pairing a two-argument async callback with owned context that is
/// lent to it (by reference) on every call.
struct WithContext<F, C> {
    handler: F,
    context: C,
}

impl<'h, T: 'h, C, F> Handler<'h, T> for WithContext<F, C>
where
    F: for<'s> ContextCallback<'h, 's, T, C>,
    C: 'h,
{
    fn call<'s>(&'s self, arg: &'h T) -> BoxFuture<'s, ()>
    where
        'h: 's,
    {
        Box::pin(ContextCallback::call(&self.handler, arg, &self.context))
    }
}

/// A registry of async handlers keyed on a shared reference to `T`.
///
/// `'h` is the lifetime of the borrows passed to [`call`](Observable::call);
/// handlers and any context they own must outlive it.
pub struct Observable<'h, T> {
    handlers: Vec<Box<dyn Handler<'h, T> + 'h>>,
}

impl<'h, T: 'h> Observable<'h, T> {
    /// Create an empty `Observable`.
    pub fn new() -> Self {
        Observable {
            handlers: Vec::new(),
        }
    }

    /// Register an async handler taking `&T`.
    ///
    /// Accepts every async-callback spelling: free/associated `async fn` items,
    /// `async` closures (`async |x: &T| { .. }`), and plain closures returning a
    /// future (`|x: &T| async move { .. }`). The returned future must be
    /// [`Send`].
    pub fn register<F, Fut>(&mut self, handler: F)
    where
        F: Fn(&'h T) -> Fut + 'h,
        Fut: Future<Output = ()> + Send + 'h,
    {
        self.handlers.push(Box::new(Plain(handler)));
    }

    /// Register an async handler taking `&T` plus a `&C` borrowed from the
    /// `context` value moved in here. The context is owned by the `Observable`
    /// and lent to the handler on every [`call`](Observable::call).
    pub fn register_with_context<F, C>(&mut self, handler: F, context: C)
    where
        F: for<'s> ContextCallback<'h, 's, T, C> + 'h,
        C: 'h,
    {
        self.handlers
            .push(Box::new(WithContext { handler, context }));
    }

    /// Invoke every registered handler with `arg`, driving each returned future
    /// to completion on the current thread.
    pub async fn call(&self, arg: &'h T) {
        futures::future::join_all(self.handlers.iter().map(|h| h.call(arg))).await;
    }
}

impl<'h, T: 'h> Default for Observable<'h, T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct ExampleType;
    #[derive(Debug)]
    struct ExampleType2;

    async fn example_handler(something: &ExampleType) {
        println!("This is just an example! {something:?}")
    }

    async fn example_handler_2(something: &ExampleType, context: &ExampleType2) {
        println!("This is just an example! {something:?}, {context:?}")
    }

    #[tokio::test]
    async fn basic_test() {
        let mut observable = Observable::<ExampleType>::new();

        // A free async fn.
        observable.register(example_handler);

        // An async closure.
        observable
            .register(async |something: &ExampleType| println!("Closures work too! {something:?}"));

        // A plain closure returning an `async` block. Under the hood, each of
        // these is stored as a type-erased `Fn(&T) -> Future`.
        observable.register(|something: &ExampleType| async move {
            println!("Under the hood, there is a type-erased 'Fn(&T) -> Future'. {something:?}")
        });

        // This runs all of the functions.
        observable.call(&ExampleType);
    }

    #[tokio::test]
    async fn test_with_context() {
        let mut observable = Observable::<ExampleType>::new();

        observable.register_with_context(example_handler_2, ExampleType2);

        // This runs all of the functions.
        observable.call(&ExampleType);
    }
}
