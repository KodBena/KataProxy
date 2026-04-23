import functools
from typing import Callable, TypeVar, Protocol

# Define the types for clarity and safety
ContextT = TypeVar('ContextT')
ResultT = TypeVar('ResultT')
OtherResultT = TypeVar('OtherResultT')

# This protocol ensures that the objects our factories produce have a .then() method.
# Your Transformer class already satisfies this.
class Composable(Protocol[OtherResultT]):
    def then(self, other: OtherResultT) -> 'Composable[OtherResultT]':
        ...

# The generic Contextual wrapper
class Contextual:
    """
    A wrapper for "contextual factories" (functions of the form `context -> result`)
    that enables Kleisli-style composition via a .then() method.
    """
    def __init__(self, factory: Callable[[ContextT], ResultT]):
        """Initializes with a factory function."""
        self.factory = factory
        # Preserve the original function's name and docstring for better introspection
        functools.update_wrapper(self, factory)

    def __call__(self, context: ContextT) -> ResultT:
        """When called, executes the factory with the given context."""
        return self.factory(context)

    def then(
        self, 
        other: 'Contextual | Callable[[ContextT], Composable]'
    ) -> 'Contextual':
        """
        Composes this contextual factory with another, returning a new one.

        The composition is lazy; the factories are only called when the
        resulting composed factory is eventually invoked with a context.
        """
        # Ensure the 'other' is also a Contextual instance for uniform handling.
        # This allows chaining with raw functions, e.g., .then(my_factory_func)
        if not isinstance(other, Contextual):
            other = Contextual(other)

        # The new, composed factory function
        def composed_factory(context: ContextT) -> Composable:
            # 1. Create the first object using the context
            first_result = self.factory(context)
            
            # 2. Create the second object using the same context
            second_result = other.factory(context)

            # 3. Compose the two resulting objects using their own .then() method
            return first_result.then(second_result)

        return Contextual(composed_factory)

# For convenient use as a decorator
contextual = Contextual
