"""
contextual.py — Kleisli-style composition for context-passing factories.

A ``Contextual[Ctx, R]`` wraps a factory function ``Ctx -> R``. When the
result type ``R`` has a ``.then(...)`` method (i.e. is itself
``Composable``), two contextual factories compose via the same operator
that the result type uses, with the shared context threaded into both.

The transformer wiring at the ``ProxyServer`` construction site is the
canonical use:

    transformer_factory = (
        Contextual(capability_gate("delta_analysis", analysis_enricher))
        .then(capability_gate("transposition", transposition_enricher))
    )

Each inner factory receives the same ``ProxyLink`` instance, so its
inner Transformer can share state with the link (e.g. cleanup on
``link.mapping.forward(eid) is None``).

License: Public Domain (Unlicense). See UNLICENSE at the project root.
"""

from __future__ import annotations

import functools
from typing import Any, Callable, Generic, Protocol, TypeVar, Union, cast


class Composable(Protocol):
    """Result types of a ``Contextual`` factory must support ``.then(other)``.

    Structurally permissive: any concrete class whose ``.then`` accepts a
    value of *some* type and returns *some* type satisfies this protocol,
    because parameter types are contravariant for protocol compliance.
    The ``Transformer`` class in ``AbstractProxy.protocol_transformer``
    satisfies this even though its ``.then`` is more specific (accepts
    only ``Transformer[Q, R]``); the runtime contract is what
    ``Contextual.then`` actually needs.
    """

    def then(self, other: Any) -> Any: ...


ContextT = TypeVar("ContextT")
ResultT = TypeVar("ResultT")


class Contextual(Generic[ContextT, ResultT]):
    """A wrapper for "contextual factories" (``context -> result``) that
    enables Kleisli-style composition via a ``.then`` method.

    The composition is lazy; the inner factories are only called when the
    resulting composed factory is eventually invoked with a context.
    """

    def __init__(self, factory: Callable[[ContextT], ResultT]) -> None:
        self.factory: Callable[[ContextT], ResultT] = factory
        # Preserve the original function's name and docstring for better
        # introspection.
        functools.update_wrapper(self, factory)

    def __call__(self, context: ContextT) -> ResultT:
        return self.factory(context)

    def then(
        self,
        other: Union[
            "Contextual[ContextT, ResultT]", Callable[[ContextT], ResultT]
        ],
    ) -> "Contextual[ContextT, ResultT]":
        """Compose this contextual factory with another, returning a new one.

        ``other`` may be a bare callable (it will be wrapped in
        ``Contextual``) or another ``Contextual``. The composed factory,
        when invoked with a context, instantiates both inner results
        against the shared context and composes them via the result
        type's own ``.then`` method.
        """
        other_contextual: Contextual[ContextT, ResultT]
        if isinstance(other, Contextual):
            other_contextual = other
        else:
            other_contextual = Contextual(other)

        def composed_factory(context: ContextT) -> ResultT:
            first_result: ResultT = self.factory(context)
            second_result: ResultT = other_contextual.factory(context)
            # ResultT is unconstrained in Contextual's generics because
            # binding it to the Composable protocol imposes variance
            # requirements that concrete result classes like
            # ``Transformer`` (whose .then is narrower than the
            # protocol's ``then(other: Any)``) cannot satisfy under
            # mypy's strict checks. The cast through ``Composable``
            # records that the method-call protocol is enforced
            # structurally; the returned value's static type is then
            # narrowed back to ``ResultT`` because the runtime contract
            # is that ``X.then(X) -> X``. ADR-0002 Rule 2: the cast is
            # documented at the only place it matters.
            first_composable = cast(Composable, first_result)
            composed = first_composable.then(second_result)
            return cast(ResultT, composed)

        return Contextual(composed_factory)


# Convenient alias for decorator-style use.
contextual = Contextual
